import json
import uuid

from kafka import KafkaProducer
import psycopg

from config import CONNECTION_URL


# класс сервиса
class CentralService:
    def __init__(self, kafka_connection_string):
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_connection_string],
            api_version=(0, 11, 5)
        )

    # отправляем сообщение в кафку
    def init_transfer(self, from_account, to_account, amount):
        id_ = str(uuid.uuid4())
        data = json.dumps({
            "id": id_,
            "from": from_account,
            "to": to_account,
            "from_currency": self.account_currency(from_account),
            "to_currency": self.account_currency(to_account),
            "amount": amount,
        }).encode("utf-8")
        self.producer.send("converts", data)
        return id_

    def account_currency(self, account_id):
        with psycopg.connect(CONNECTION_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT currency FROM accounts WHERE user_id = %s",
                    (account_id,)
                )
                return cur.fetchone()[0]

    def get_accounts(self):
        with psycopg.connect(CONNECTION_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT user_id, balance, currency FROM accounts",
                )
                return cur.fetchall()
