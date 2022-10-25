import datetime
import logging
import psycopg2


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


# функция для отправки запросов к бд
def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        logging.info("Query executed successfully")
    except psycopg2.OperationalError as e:
        logging.error(f"The error '{e}' occurred")


def del_table():
    connection = create_connection('airflow', 'airflow', 'airflow', '127.0.0.1', '5432')
    # отправка запроса на удаление таблицы
    delete_comment = "DROP TABLE sessions"
    execute_query(connection, delete_comment)

    delete_comment = "DROP TABLE filelist"
    execute_query(connection, delete_comment)

    delete_comment = "DROP TABLE hits"
    execute_query(connection, delete_comment)

    connection.close()


if __name__ == '__main__':
    del_table()

