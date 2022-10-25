import psycopg2
import logging
import pandas as pd
import datetime as dt
from pathlib import Path
import os

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer

import numpy as np
from psycopg2.extensions import register_adapter, AsIs

register_adapter(np.int64, AsIs)

PATH = os.environ.get('PROJECT_PATH', '.')


def time_now():
    dtm = dt.datetime.now()

    return dtm.strftime("%d/%m/%Y %H:%M:%S")


# функция для подключения к бд
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


# отбрасываем все ненужные признаки
def filter_data(df: pd.DataFrame) -> pd.DataFrame:
    if len(df.columns) > 15:
        columns_to_drop = ['device_model', 'utm_source', 'utm_campaign', 'utm_adcontent',
                   'utm_keyword', 'device_screen_resolution', 'client_id']
    else:
        columns_to_drop = ['event_value', 'hit_page_path', 'hit_time',
                                'hit_referer', 'event_label']

    return df.drop(columns=columns_to_drop, axis=1)


# все необходимые преобрвзования над значениями в датафрейме
def values_update(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2.session_id = df2.session_id.apply(lambda x: int(x.replace('.', '')))

    if len(df2.columns) > 9:
        special_mask = ((df2.device_brand == '(not set)') &
                        ((df2.device_os.isna()) | (df2.device_os == 'Android') |
                        (df2.device_category == 'mobile')))
        df2.loc[special_mask, 'device_brand'] = 'Xiaomi'

        df2.loc[df.device_browser == 'Safari (in-app)', 'device_browser'] = 'Safari'
        df2.loc[df.device_brand.isna(), 'device_brand'] = '(not set)'
        df2.loc[(df.device_brand == 'Apple') & (df2.device_os.isna()), 'device_os'] = 'iOS'

        brand_keys = list(df2.device_brand.value_counts()[:20].keys())
        brand_keys.remove('Apple')
        brand_keys.remove('(not set)')

        for i in range(len(df)):
            if df2.device_brand[i] in brand_keys:
                df2.loc[i:i, 'device_os'] = 'Android'

        df2.loc[((df2.device_category == 'desktop') &
                         (df2.device_os.isna())), 'device_os'] = 'Windows'

        df2.loc[df.utm_medium == '(none)', 'utm_medium'] = '(not set)'
    else:
        df2.hit_date = df2.hit_date.apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d'))

    return df2


# создание нового признака visit_dt
def add_visit_dt(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    if len(df.columns) > 9:
        df2 = df.copy()
        for i in range(len(df2)):
            row_date = dt.datetime.strptime(df2.visit_date[i] + ' ' + df2.visit_time[i],
                                            '%Y-%m-%d %H:%M:%S')
            df2.loc[i:i, 'visit_dt'] = row_date
        df2.drop(columns=['visit_date', 'visit_time'], axis=1, inplace=True)

    return df2


# загрузка json файлов в бд sessions
def load_json(df: pd.DataFrame) -> None:
    df2 = df.copy()

    df2_list = []
    for i in range(len(df2)):
        df2_list.append(tuple(df2.iloc[i].values))

    sessions_records = ", ".join(["%s"] * len(df2_list))
    if len(df2.columns) > 9:
        insert_query = (
            f"INSERT INTO sessions (session_id, visit_number,"
            f"utm_medium, device_category, device_os, device_brand,"
            f"device_browser, geo_country, geo_city, visit_dt) VALUES {sessions_records}"
        )
    else:
        insert_query = (
            f"INSERT INTO hits (session_id, hit_date, hit_number,"
            f"hit_type, event_category, event_action) VALUES {sessions_records}"
        )

    connection = create_connection('airflow', 'airflow', 'airflow', 'pg_db', '5432')
    cursor = connection.cursor()

    cursor.execute(insert_query, df2_list)
    connection.commit()

    cursor.close()
    connection.close()

# загрузка информации о загружаемых файлах в таблицу filelist
def loading_to_filelist(connection, name_file, length):
    time_load = time_now()
    len_file = length
    # список для загрузки в таблицу filelist
    file_list = [(name_file, len_file, time_load)]

    # загрузка данных в таблицу filelist о загруженных данных
    records = ", ".join(["%s"] * len(file_list))
    insert_query = (
        f"INSERT INTO filelist (file_name, file_long, time_load) VALUES {records}")

    cursor = connection.cursor()
    cursor.execute(insert_query, file_list)
    connection.commit()
    cursor.close()


# смотрим наличие новых json файлов в "./data/json/"
def list_file_path():
    list_load = []
    path_json = f"{PATH}/data/json"
    for txt in Path(path_json).glob("*.json"):
        answer = txt.name.find('2')
        name_col = txt.name[answer:answer + 10]
        list_load.append((path_json + '/' + txt.name, txt.name, name_col))

    return list_load


# функция чтения информации с таблицы
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


# отправка запроса в filelist на наличия файла в sessions
def request_to_filelist(connection, name_file):
    select_sessions = "SELECT * FROM filelist"
    file_list = execute_read_query(connection, select_sessions)

    list_name_file = []
    for file in file_list:
        list_name_file.append(file[0])

    if name_file in list_name_file:
        return 0
    else:
        return 1


def pipeline() -> None:
    # подключаемся к нашей бд
    connection = create_connection('airflow', 'airflow', 'airflow', 'pg_db', '5432')
    # смотрим какие json файлы есть у нас в ./data/json
    list_jsons = list_file_path()
    for i in list_jsons:
        if request_to_filelist(connection, i[1]):
            pre_df = pd.read_json(i[0])[i[2]]
            if len(pre_df):
                df = pd.DataFrame(columns=list(pre_df[0].keys()), data=[pre_df[x].values() for x in range(len(pre_df))])

                preprocessor = Pipeline(steps=[
                    ('filter', FunctionTransformer(filter_data)),
                    ('visit_dt', FunctionTransformer(add_visit_dt)),
                    ('values', FunctionTransformer(values_update)),
                    ('load_json', FunctionTransformer(load_json)),
                    ('load_to_filelist', FunctionTransformer(loading_to_filelist(connection, i[1], len(df))))
                ])

                preprocessor.fit_transform(df)

            else:
                logging.info(f"Файл {i[1]} не содержит данных")
        else:
            logging.info(f'Файл {i[1]} уже загружен')

    connection.close()


if __name__ == '__main__':
    pipeline()

