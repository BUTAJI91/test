import os
import datetime
import httplib2
import psycopg2
import urllib.request
import xml.etree.ElementTree as Et

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


def create_connection(db_name, db_user, db_password, db_host, db_port):
    conn = None
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except psycopg2.OperationalError as error:
        print(f"The error '{error}' occurred")
    return conn


def create_database(conn, query):
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except psycopg2.OperationalError as error:
        print(f"The error '{error}' occurred")


def execute_query(conn, query, post=None):
    if post is None:
        post = []
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        cursor.execute(query, post)
        print("Query executed successfully")
        return cursor
    except psycopg2.OperationalError as error:
        print(f"The error '{error}' occurred")


def get_service_acc():
    cred_json = os.path.dirname(__file__) + "/credentials.json"
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    cred_service = ServiceAccountCredentials.from_json_keyfile_name(cred_json, scopes).authorize(httplib2.Http())
    return build('sheets', 'v4', http=cred_service)


service_acc = get_service_acc()
sheet = service_acc.spreadsheets()

sheet_id = "1dc4FUHfKT3qoD0qzNNWRhT-CvsHDrjJqRFNTKkjUwFI"  # Идентификатор таблицы

#   Получаем данные листа из таблицы Google и удаляем заголовок
data = sheet.values().get(spreadsheetId=sheet_id, range="Лист1").execute()["values"]
data.pop(0)

#   Берем XML файл и парсим для обработки
xml = urllib.request.urlopen('https://www.cbr.ru/scripts/XML_daily.asp')
root = Et.parse(xml).getroot()

#   Ищем доллар, берем его курс и переводим в вещественный тип
for elem in root:
    if elem.attrib["ID"] == "R01235":
        rate = float(elem[4].text.replace(",", "."))
        break

#   Подключаемся к БД и берем все записи
connection = create_connection("test", "postgres", "123456", "127.0.0.1", "5432") # Пример данных авторизации
DB_data = execute_query(connection, "SELECT * from test").fetchall()

'''
if not connection:
    connection = create_connection("postgres", "postgres", "306xz20J", "127.0.0.1", "5432")
    create_database_query = "CREATE DATABASE test"
    create_database(connection, create_database_query)
    connection = create_connection("test", "postgres", "123456", "127.0.0.1", "5432") # Пример данных авторизации

try:
    check_test_table = "SELECT pg_relation_size('test')"
    execute_query(connection, check_test_table)
except Exception as e:
    print(f"The error '{e}' occurred")
    create_test_table = """
    CREATE TABLE IF NOT EXISTS test (
      id SERIAL PRIMARY KEY,
      order_number INTEGER NOT NULL, 
      cost_in_dollars float,
      cost_in_rubles float,
      delivery_time date
    )
"""

execute_query(connection, create_test_table)
'''

list_i = []  # Будущий список индексов всех актуальных строк БД (нужен для удаления отсутствующих)
for elem in data:

    '''
        Преобразуем данные массива.
        Удаляем ID, получаем рубли с учетом курса, приводим дату в формат datetime.date.
    '''
    elem.pop(0)
    elem[1] = float(elem[1])
    elem.insert(2, rate * elem[1])
    elem[3] = datetime.date(
        int(elem[3][6:]),
        int(elem[3][3:5]),
        int(elem[3][:2])
    )

    '''
        Проверяем актуальность данных в БД.
        Если строка неактуальна, то изменяем ее в БД.
    '''
    find = False  # Переменная для факта нахождения строки из таблицы Google в БД
    for i, DB_elem in enumerate(DB_data):
        if elem[0] == DB_elem[1]:

            find = True  # Строка есть в БД
            list_i.append(i)  # Пишем индекс строки (нужно для удаления строк БД, удаленных из Google)

            if elem[1] != DB_elem[2]:  # Если стоимость заказа была изменена
                update_cost = f"""
                    UPDATE test
                    SET cost_in_dollars = %s and cost_in_rubles = %s
                    WHERE id = {DB_elem[0]}
                """
                execute_query(connection, update_cost, [elem[1], elem[2]])
                # print("Стоимость заказа изменена: ", elem)

            if elem[2] != DB_elem[4]:  # Если срок поставки был изменен
                update_delivery_time = f"""
                    UPDATE test
                    SET delivery_time = %s
                    WHERE id = {DB_elem[0]}
                """
                execute_query(connection, update_delivery_time, elem[3])
                # print("Срок поставки изменен: ", elem)
                # Мог написать отправку в ТГ, но пришлось пропустить из-за времени

    if not find:
        '''
            Пишем строку в БД.
            Половину дня пытался сделать одним запросом, что-то пошло не так.
        '''
        # print("Новая строка: ", elem)
        values = ", ".join(["%s"] * len(elem))
        insert_test_table = f"INSERT INTO test (order_number, cost_in_dollars, cost_in_rubles, delivery_time) VALUES ({values})"
        execute_query(connection, insert_test_table, elem)

# print(data)
# print(DB_data)

id_delete_str = []

#   Удаляем строки БД, которых уже нет в таблице Google
for i, elem in enumerate(DB_data):
    if i not in list_i:
        id_delete_str.append(elem[0])
        # print("Строка для удаления: ", elem)

execute_query(connection, "DELETE FROM test WHERE id = %s", id_delete_str)
