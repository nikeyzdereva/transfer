import httpx
from httpx import Timeout


# класс сервиса
class ConverterService:
    def __init__(self, api_key):
        self.api_key = api_key
        self.currencies = {}

    # сохраняем значение курса одной валюты к другой
    def save_currency(self, from_, to_, value):
        if self.currencies.get(from_) is None:
            self.currencies[from_] = {}
        self.currencies[from_][to_] = value

    # получаем через сторонний апи значение курса
    async def currency_exchange_rate(self, from_, to_):
        url = f"https://api.apilayer.com/currency_data/convert?to={to_}&from={from_}&amount=1"
        headers = {"apikey": self.api_key}
        timeout = Timeout(timeout=30.0)
        response = httpx.get(url, headers=headers, timeout=timeout)

        print(response.json())
        value = response.json()["result"]

        return value

    # обновляем все отношения курсов
    async def update_currency(self, currencies):
        for i in currencies:
            for j in currencies:
                if i == j:
                    value = 1.0
                else:
                    value = await self.currency_exchange_rate(i, j)
                self.save_currency(i, j, value)
        return self.currencies
