Тестовое задание на позицию "data engineer"

Приложение позволяет хранить текущие курсы рублей, долларов, евро и юаней, а также историю курсов за выбранный период (для примера взят диапазон с 01.07.2021)

Для запуска приложения необходима СУБД MySQL и созданная в ней база данных "exchange_db".

Для выполнения задач по расписанию и периодических задач используется Celery, брокером являетя redis.
Если redis не установлен, можно скачать docker-образ с docker-hub командой "docker pull redis" и перед запуском приложения в командной строке ввести команду
"docker-compose up". Celery взаимодействует с redis через порт 6379.

В файле config.py необходимо указать параметры подключения к базе данных: имя пользователя, пароль, хост, имя базы данных.

Для создания таблиц и заполнения их данными необходимо запустить файл db.py.

Приложение запускается командой "celery -A main.app worker -B".

Актуализация текущего курса валют производится с периодичностью 15 минут.
Актуализация слоя Data Marts также каждые 15 минут.
Актуализация истории курсов валют производится каждый день в 8:00 утра.

Слой Core состоит из таблиц:
- currency (хранит обозначения валют - RUB, USD, EUR, CNY)
- language (хранит языки, на которые переводятся названия валют)
- currency_translate (хранит связи между обозначениями валют и языком, а также переводы валют на каждый язык)
- exchange_rate_latest (хранит текущие курсы валют)
- exchange_rate_history (хранит историю курсов валют)

Слой Data Marts состоит из таблиц:
- exchange_rate (хранит историю и текущие курсы с обозначениями валют)
- currency_designation (хранит обозначения валют)
- currency_name (хранит обозначения валют? языки и переводы названия валют на различных языках)

![alt text](https://github.com/KleachiM/data_engineer/blob/main/exchange_db.png)