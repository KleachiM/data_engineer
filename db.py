"""
Модуль предназначен для создания таблиц в базе данных.
"""
import datetime

import pymysql

from config import host, user, password, db_name
from api import api_request, URL


def db_query(query, select=False):
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )

        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                if not select:
                    connection.commit()
                    return 'success'
                else:
                    return cursor.fetchall()

        finally:
            connection.close()

    except Exception as ex:
        print(ex)
        return 'error'


def create_all_tables():

    # создание таблицы currency (хранит обозначение валюты (RUB, USD, EUR, CNY))
    db_query("CREATE TABLE currency "
             "(id int AUTO_INCREMENT, "
             "designation varchar(32) NOT NULL, "
             "PRIMARY KEY(id));")

    # создание таблицы language (хранит языки, на которые нужно переводить названия валют)
    db_query("CREATE TABLE language "
             "(id int AUTO_INCREMENT, "
             "name varchar(32) NOT NULL, "
             "PRIMARY KEY (id));")

    # создание таблицы currency_translate (хранит названия валют на разных языках)
    db_query("CREATE TABLE currency_translate "
             "(currency_id int NOT NULL, "
             "language_id int NOT NULL, "
             "translate varchar(32) NOT NULL, "
             "FOREIGN KEY (currency_id) REFERENCES currency (id), "
             "FOREIGN KEY (language_id) REFERENCES language (id));")

    # # создание таблицы exchange_rate_today (хранит текущий курс валют)
    db_query("CREATE TABLE exchange_rate_latest "
             "(date date, "
             "currency_id int NOT NULL, "
             "rate float NOT NULL, "
             "PRIMARY KEY (date, currency_id), "
             "FOREIGN KEY (currency_id) REFERENCES currency (id));")

    # создание таблицы exchange_rate_history (хранит историю курсов валют)
    db_query("CREATE TABLE exchange_rate_history "
             "(date date , "
             "currency_id int NOT NULL, "
             "rate float NOT NULL, "
             "PRIMARY KEY (date, currency_id), "
             "FOREIGN KEY (currency_id) REFERENCES currency (id));")

    # создание таблицы currency_designation (хранит обозначения валют)
    db_query("CREATE TABLE currency_designation "
             "(designation varchar(32) UNIQUE NOT NULL);")

    # создание таблицы currency_name (хранит обозначения валют и переводы на различные языки)
    db_query("CREATE TABLE currency_name "
             "(designation varchar(32) NOT NULL, "
             "language varchar(32) NOT NULL, "
             "name varchar(32) NOT NULL, "
             "FOREIGN KEY (designation) REFERENCES currency_designation (designation));")

    # создание таблицы exchange_rate (хранит курсы валют по отношению друг к другу на определенную дату)
    db_query("CREATE TABLE exchange_rate "
             "(date date, "
             "currency_designation_from varchar(32), "
             "currency_designation_to varchar(32), "
             "rate float NOT NULL, "
             "FOREIGN KEY (currency_designation_from) REFERENCES currency_designation (designation), "
             "FOREIGN KEY (currency_designation_to) REFERENCES currency_designation (designation), "
             "PRIMARY KEY (date, currency_designation_from, currency_designation_to));")


def fill_tables():

    # заполнение таблицы language языками
    db_query("INSERT INTO language (name)"
             "VALUES ('русский'), ('английский'), ('немецкий'), ('китайский');")

    # заполнение таблицы currency обозначениями валют
    db_query("INSERT INTO currency (designation)"
             "VALUES ('RUB'), ('USD'), ('EUR'), ('CNY');")

    # заполнение таблицы currency_translate переводами валют на разные языки
    db_query("INSERT INTO currency_translate (currency_id, language_id, translate)"
             "VALUES "
             "(1, 1, 'рубль'), (2, 1, 'доллар'), (3, 1, 'евро'), (4, 1, 'юань'), "
             "(1, 2, 'rouble'), (2, 2, 'dollar'), (3, 2, 'euro'), (4, 2, 'yuan'), "
             "(1, 3, 'rubel'), (2, 3, 'dollar'), (3, 3, 'euro'), (4, 3, 'yuan'), "
             "(1, 4, '卢布'), (2, 4, '美元'), (3, 4, '欧元'), (4, 4, '元');")

    # заполнение таблицы exchange_rate_history данными с http://exchangeratesapi.io с 01.07.2021
    start_date = datetime.date(2021, 7, 1)
    number_of_days = datetime.datetime.now().day

    # создание списка дат
    date_list = []
    for day in range(number_of_days):
        a_date = (start_date + datetime.timedelta(days=day)).isoformat()
        date_list.append(a_date)

    for date in date_list:
        try:
            rates = api_request(URL, period=date)['rates']
            db_query(f"INSERT INTO exchange_rate_history (date, currency_id, rate)"
                     f"VALUES "
                     f"('{date}', 1, {rates['RUB']}), "
                     f"('{date}', 2, {rates['USD']}), "
                     f"('{date}', 3, {rates['EUR']}), "
                     f"('{date}', 4, {rates['CNY']});")
        except KeyError:
            print(f'отсутствуют данные за {date}')

    # заполнение таблицы exchange_rate_today (данные о конвертации валют на текущий момент)
    date = api_request(URL)['date']
    rates = api_request(URL)['rates']

    db_query(f"INSERT INTO exchange_rate_latest (date, currency_id, rate) "
             f"VALUES "
             f"('{date}', 1, {rates['RUB']}), "
             f"('{date}', 2, {rates['USD']}), "
             f"('{date}', 3, {rates['EUR']}), "
             f"('{date}', 4, {rates['CNY']});")


if __name__ == '__main__':
    create_all_tables()
    fill_tables()
