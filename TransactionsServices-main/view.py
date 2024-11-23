from threading import Thread

import customtkinter as ctk
from tkinter import messagebox

from kafka import KafkaConsumer

import requests

# Настройка темы
ctk.set_appearance_mode("dark")  # "dark", "light", "system"
ctk.set_default_color_theme("blue")  # "blue", "green", "dark-blue"


def get_accounts():
    url = "http://127.0.0.1:8081/accounts"
    res = requests.get(url).json()["accounts"]

    accs = []

    for i in res:
        accs.append(
            f"id: {i['id']}, balance: {i['balance']}, currency: {i['currency']}"
        )

    return accs


def init_transfer(from_, to_, amount):
    url = (
        f"http://127.0.0.1:8081/init_transfer?"
        f"from_account={from_}&"
        f"to_account={to_}&"
        f"amount={amount}"
    )
    return requests.get(url).json()["transfer_id"]


# Создаем основное окно
class MoneyTransferApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Перевод средств")
        self.geometry("600x600")

        # Создание виджетов
        self.create_widgets()

    def update_accounts(self):
        accs = get_accounts()
        data = {"values": accs}
        self.sender_account.configure(require_redraw=True, **data)
        self.recipient_account.configure(require_redraw=True, **data)

    def create_widgets(self):
        # Заголовок
        self.label = ctk.CTkLabel(self, text="Перевод средств", font=("Arial", 20))
        self.label.pack(pady=10)

        # Выбор аккаунта отправителя
        self.sender_account_label = ctk.CTkLabel(self, text="Ваш аккаунт:")
        self.sender_account_label.pack(pady=5)

        accs = get_accounts()
        self.sender_account = ctk.CTkOptionMenu(self, values=accs)
        self.sender_account.pack(pady=5)

        # Выбор аккаунта получателя
        self.recipient_account_label = ctk.CTkLabel(self, text="Аккаунт для перевода:")
        self.recipient_account_label.pack(pady=5)

        self.recipient_account = ctk.CTkOptionMenu(self, values=accs)
        self.recipient_account.pack(pady=5)

        # Сумма перевода
        self.amount_label = ctk.CTkLabel(self, text="Сумма перевода:")
        self.amount_label.pack(pady=5)

        self.amount_entry = ctk.CTkEntry(self)
        self.amount_entry.pack(pady=5)

        # Кнопка для перевода
        self.transfer_button = ctk.CTkButton(self, text="Перевести", command=self.show_loading)
        self.transfer_button.pack(pady=20)

        # Создание модального окна для загрузки
        self.loading_window = None

    def show_loading(self):
        # Показать модальное окно загрузки
        self.loading_window = ctk.CTkToplevel(self)
        self.loading_window.title("Загрузка")
        self.loading_window.geometry("200x100")
        self.loading_window.attributes('-topmost', True)  # Окно всегда сверху
        self.loading_label = ctk.CTkLabel(self.loading_window, text="Идет обработка перевода...")


        new_thread = Thread(target=self.get_response)  # Создаём поток
        new_thread.start()

    def get_response(self):
        consumer = KafkaConsumer(
            'result',
            bootstrap_servers=["localhost:9092"]
        )
        from_ = self.sender_account._current_value.split(", ")[0].replace("id: ", "")
        to_ = self.recipient_account._current_value.split(", ")[0].replace("id: ", "")
        amount = float(self.amount_entry.get())
        transaction_id = init_transfer(from_, to_, amount)

        for mes in consumer:
            print(mes)
            print(mes.key.decode('utf-8'))
            print(transaction_id)
            status = mes.value.decode('utf-8')
            break
        self.show_success(status)

    def show_success(self, status):
        # Закрыть окно загрузки
        self.loading_window.destroy()
        self.update_accounts()

        # Показать сообщение об успехе
        messagebox.showinfo("Перевод средств", status)

# Запускаем приложение
if __name__ == "__main__":
    app = MoneyTransferApp()
    app.mainloop()
