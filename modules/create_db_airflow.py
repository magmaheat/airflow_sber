import datetime
import logging
import psycopg2
import os

import numpy as np
from psycopg2.extensions import register_adapter, AsIs
register_adapter(np.int64, AsIs)

#PATH = '/home/epishcom'
PATH = os.environ.get('PROJECT_PATH', '.')


def time_now():
    dt = datetime.datetime.now()
    dt_string = dt.strftime("%d/%m/%Y %H:%M:%S")

    return dt_string


#  функция для подключения к бд
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        logging.info("Connection to PostgreSQL DB successful")
    except psycopg2.OperationalError as e:
        logging.error(f"The error '{e}' occurred")
    return connection


# функция для создания таблицы
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        logging.info("Query executed successfully")
        connection.commit()
        cursor.close()
    except psycopg2.OperationalError as e:
        logging.error(f"The error '{e}' occurred")
        cursor.close()


# функция для отправки запросов к бд, без ответа
def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        logging.info("Query executed successfully")
        cursor.close()
    except psycopg2.OperationalError as e:
        logging.error(f"The error '{e}' occurred")
        cursor.close()

# отправляем запрос к таблице и получаем ответ в виде списка
def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        connection.commit()
        cursor.close()
        return result
    except psycopg2.OperationalError as e:
        cursor.close()
        logging.error(f"The error '{e}' occurred")


# запросы на создание таблиц
create_hits_table = '''
CREATE TABLE IF NOT EXISTS hits (
    sessions_id NUMERIC,
    hit_date TIMESTAMP,
    hit_number INT,
    hit_type TEXT,
    event_category TEXT,
    event_action TEXT)
    '''

create_sessions_table = '''
CREATE TABLE IF NOT EXISTS sessions (
    session_id NUMERIC PRIMARY KEY,
    visit_number INTEGER,
    utm_medium TEXT,
    device_category TEXT,
    device_os TEXT,
    device_brand TEXT,
    device_browser TEXT,
    geo_country TEXT,
    geo_city TEXT,
    visit_dt TIMESTAMP)
    '''

create_filelist_table = '''
CREATE TABLE IF NOT EXISTS filelist (
    file_name TEXT,
    file_long INT,
    time_load TEXT)
    '''

# создаем таблицы подгружаем данные
def create_db_airflow():
    # подключаемся к нашей бд
    connection = create_connection('airflow', 'airflow', 'airflow', 'pg_db', '5432')
    select = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='sessions'"
    answer = execute_read_query(connection, select)

    if not answer:
        # создаем две таблицы
        logging.info(f"ответ execute_read_query: {answer}, начинается создание таблиц")
        create_database(connection, create_hits_table)
        create_database(connection, create_sessions_table)
        create_database(connection, create_filelist_table)

        # загрузка основного файла с данными
        name_files = ['df_hits.csv', 'df_sessions.csv']
        path_files = f'{PATH}/data/'

        for i in name_files:
            cursor = connection.cursor()
            copy_sql = f"""
                       COPY {i[3:-4]} FROM stdin WITH CSV HEADER
                       DELIMITER as ','
                       """

            logging.info(f"path i: {path_files+i}")
            with open(path_files + i, 'r') as f:
                cursor.copy_expert(sql=copy_sql, file=f)
                logging.info(f"Файл {i} загружен.")

            # создание файла с информацией о загруженном файле
            with open(path_files + i, 'r') as f:
                len_csv = sum(1 for _ in f)
                time_load = time_now()
                file_list = [(i, len_csv, time_load)]
                filename_records = ", ".join(["%s"] * len(file_list))
                insert_query = (
                    f"INSERT INTO filelist (file_name, file_long, time_load)    VALUES {filename_records}"
                )

                # отправляем информацию о последнем загруженном файле
                cursor.execute(insert_query, file_list)
                logging.info(f"Информация о файле {i} загружена.\n")

            connection.commit()
            cursor.close()
    else:
        logging.info("pass load file")
    connection.close()


def del_table():
    connection = create_connection('airflow', 'airflow', 'airflow', 'pg_db', '5432')
    table_list = ['sessions', 'hits', 'filelist']

    # отправка запроса на удаление таблицы
    for i in table_list:
        delete_comment = f"DROP TABLE {i}"
        execute_query(connection, delete_comment)

    connection.close()


if __name__ == '__main__':
    create_db_airflow()
    #del_table()

