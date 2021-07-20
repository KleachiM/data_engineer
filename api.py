"""
Модуль предназначен для установки параметров запросов к http://exchangeratesapi.io
"""
import requests

URL = 'http://api.exchangeratesapi.io/v1/'
ACCESS_KEY = 'b51f6396796e6b9c24d1555f6956da37'
SYMBOLS = 'RUB,USD,EUR,CNY'

params = {
    'access_key': ACCESS_KEY,
    'symbols': SYMBOLS
}


def api_request(URL, period='latest'):
    URL += period
    res = requests.get(URL, params)
    return res.json()


if __name__ == '__main__':
    print(api_request(URL, period='2021-07-20'))
