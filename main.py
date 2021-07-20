import datetime

from celery import Celery
from celery.schedules import crontab

from db import db_query
from api import api_request, URL

app = Celery(broker='redis://localhost:6379')


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Выполнение запроса на обновление данных о текущий курсах обмена валют каждые 15 минут
    # Для того, что бы запрос выполнялся каждую минуту, нужно указать 'crontab()'
    sender.add_periodic_task(
        crontab(minute='*/15'),
        exchange_latest_update.s(),
    )

    # обновление слоя Data Marts каждые 15 минут
    sender.add_periodic_task(
        crontab(minute='*/15'),
        actualize_data_marts.s()
    )

    # Выполнение запроса каждый день в 8 утра (для обновления данных по курсам за прошлый день)
    # Разница с московским временем - 3 ч (например, чтобы сработало в 18:00 нужно указать 15:00)
    sender.add_periodic_task(
        crontab(minute=0, hour='5'),
        exchange_history_update.s(),
    )


# обновление данных о текущих курсах обмена валют
@app.task
def exchange_latest_update():
    date = api_request(URL)['date']
    rates = api_request(URL)['rates']

    currencies = db_query("SELECT * FROM currency;", select=True)
    for currency in currencies:
        designation = currency['designation']
        currency_id = currency['id']
        rate = rates[f'{designation}']
        db_query(f"UPDATE exchange_rate_latest "
                 f"SET date = '{date}', rate = {rate} "
                 f"WHERE currency_id = {currency_id};")


# добавление записей в историю курсов обмена валют
@app.task
def exchange_history_update():
    date = datetime.datetime.now().date() - datetime.timedelta(days=1)
    date = date.strftime('%Y-%m-%d')

    rates = api_request(URL, period=date)
    currencies = db_query("SELECT * FROM currency;", select=True)
    for currency in currencies:
        designation = currency['designation']
        currency_id = currency['id']
        rate = rates[f'{designation}']
        db_query(f"INSERT INTO exchange_rate_history (date, currency_id, rate) "
                 f"VALUES ('{date}', {currency_id}, {rate});")


# заполнение таблицы currency_designation (в слое Data Marts хранит обозначения валют)
def fill_currency_designation():
    db_query("DELETE FROM currency_designation;")
    designations = db_query("SELECT * FROM currency;", select=True)
    for designation in designations:
        db_query(f"INSERT INTO currency_designation (designation) "
                 f"VALUES ('{designation['designation']}');")


# заполнение таблицы currency_name (в слое Data Marts хранит обозначения валют,
# языки и переводы валют на каждый язык)
def fill_currency_name():
    db_query("DELETE FROM currency_name;")
    translates = db_query("SELECT * FROM currency_translate;", select=True)
    for translate in translates:
        name = translate['translate']
        designation = db_query(f"SELECT (designation) FROM currency "
                               f"WHERE currency.id = {translate['currency_id']}", select=True)
        designation = designation[0]['designation']
        language = db_query(f"SELECT (name) FROM language "
                            f"WHERE language.id = {translate['language_id']}", select=True)
        language = language[0]['name']
        db_query(f"INSERT INTO currency_name (designation, language, name) "
                 f"VALUES ('{designation}', '{language}', '{name}');")


# актуализация данных в таблице exchange_rate (слой Data Marts, хранит историю и текущие курсы валют)
def exchange_rate_make():
    db_query("DELETE FROM exchange_rate;")

    # сначала добавляются данные из истории
    dates = db_query("SELECT DISTINCT date FROM exchange_rate_history;", select=True)
    for date in dates:
        date = date['date'].strftime('%Y-%m-%d')

        rates_in_date = db_query(f"SELECT * FROM exchange_rate_history "
                                 f"WHERE date = '{date}';", select=True)
        for rate_i in rates_in_date:
            for rate_j in rates_in_date:
                currency_designation_from = db_query(f"SELECT (designation) FROM currency "
                                                     f"WHERE currency.id = {rate_i['currency_id']};",
                                                     select=True)
                currency_designation_from = currency_designation_from[0]['designation']
                currency_designation_to = db_query(f"SELECT (designation) FROM currency "
                                                   f"WHERE currency.id = {rate_j['currency_id']};",
                                                   select=True)
                currency_designation_to = currency_designation_to[0]['designation']
                rate = rate_i['rate'] / rate_j['rate']
                db_query(f"INSERT INTO exchange_rate (date, currency_designation_from, currency_designation_to, rate) "
                         f"VALUES ('{date}', '{currency_designation_from}', '{currency_designation_to}', {rate});")

    # затем добавляются данные о текущих курсах
    rates_latest = db_query("SELECT * FROM exchange_rate_latest;", select=True)
    date = rates_latest[0]['date']
    date = date['date'].strftime('%Y-%m-%d')

    for rate_i in rates_latest:
        for rate_j in rates_latest:
            currency_designation_from = db_query(f"SELECT (designation) FROM currency "
                                                 f"WHERE currency.id = {rate_i['currency_id']};",
                                                 select=True)
            currency_designation_from = currency_designation_from[0]['designation']
            currency_designation_to = db_query(f"SELECT (designation) FROM currency "
                                               f"WHERE currency.id = {rate_j['currency_id']};",
                                               select=True)
            currency_designation_to = currency_designation_to[0]['designation']
            rate = rate_i['rate'] / rate_j['rate']
            db_query(f"INSERT INTO exchange_rate (date, currency_designation_from, currency_designation_to, rate) "
                     f"VALUES ('{date}', '{currency_designation_from}', '{currency_designation_to}', {rate});")


# актуализация данных в слое Data Marts
def actualize_data_marts():
    fill_currency_designation()
    fill_currency_name()
    exchange_rate_make()
