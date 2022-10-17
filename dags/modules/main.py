import datetime
import logging
import psycopg2

PATH = '/home/epishcom'


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


# функция для отправки запросов к бд
def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        logging.info("Query executed successfully")
    except psycopg2.OperationalError as e:
        logging.error(f"The error '{e}' occurred")


# запрос на создание таблицы в бд
create_sessions_table = '''
CREATE TABLE IF NOT EXISTS sessions (
    session_id NUMERIC,
    visit_number INTEGER,
    utm_medium TEXT,
    device_category TEXT,
    device_os TEXT,
    device_brand TEXT,
    device_browser TEXT,
    geo_country TEXT,
    geo_city TEXT,
    visit_dt TIMESTAMP
    )
    '''


# запрос на создание таблицы с загружаемыми файлами
create_filelist_table ='''
CREATE TABLE IF NOT EXISTS filelist (
    file_name TEXT,
    file_long INT,
    time_load TEXT)'''


def main():
    # подключаемся к нашей бд
    connection = create_connection('my_db', 'root', 'root', '127.0.0.1', '5432')

    # создаем две таблицы
    create_database(connection, create_sessions_table)
    create_database(connection, create_filelist_table)

    # загрузка основного файла с данными
    file = f'{PATH}/diploma/data/df_sessions.csv'
    name_file = file[-file[::-1].find('/'):]

    copy_sql = """
               COPY sessions FROM stdin WITH CSV HEADER
               DELIMITER as ','
               """

    connection.autocommit = True
    cursor = connection.cursor()

    with open(file, 'r') as f:
        cursor.copy_expert(sql=copy_sql, file=f)
        logging.info(f"Файл {name_file} загружен.")

    # создание файла с информацией о загруженном файле
    with open(file, 'r') as f:
        len_csv = sum(1 for _ in f)
        time_load = time_now()
        file_list = [(name_file, len_csv, time_load)]
        filename_records = ", ".join(["%s"] * len(file_list))
        insert_query = (
            f"INSERT INTO filelist (file_name, file_long, time_load) VALUES {filename_records}"
        )

        # отправляем информацию о последнем загруженном файле
        cursor.execute(insert_query, file_list)
        logging.info(f"Информация о файле {name_file} загружена.\n")

    cursor.close()
    connection.close()


def del_table():
    connection = create_connection('my_db', 'root', 'root', '127.0.0.1', '5432')
    # отправка запроса на удаление таблицы
    delete_comment = "DROP TABLE sessions"
    execute_query(connection, delete_comment)

    delete_comment = "DROP TABLE filelist"
    execute_query(connection, delete_comment)

    connection.close()


if __name__ == '__main__':
    main()
    #del_table()

