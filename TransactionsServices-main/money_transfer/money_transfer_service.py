import json

from kafka import KafkaProducer, KafkaConsumer
import psycopg

from config import CONNECTION_URL, KAFKA_CONNECTION_STRING


# класс сервиса
class MoneyTransferService:
    def transfer(self, transaction_id, from_account, to_account, from_amount, to_amount):
        from_account_balance = self.get_account_balance(from_account)
        to_account_balance = self.get_account_balance(to_account)
        status = "SUCCEFULL"

        from_amount = float(from_amount)
        to_amount = float(to_amount)

        with psycopg.connect(CONNECTION_URL) as conn:
            with conn.cursor() as cur:
                if from_account_balance < from_amount:
                    status = "ERROR"
                else:
                    self.update_balance(
                        from_account_balance - from_amount,
                        from_account,
                        cur
                    )
                    self.update_balance(
                        to_account_balance + to_amount,
                        to_account,
                        cur
                    )
                cur.execute(
                    "INSERT INTO transactions(id, from_id, to_id, amount, status) VALUES(%s, %s, %s, %s, %s)",
                    (transaction_id, from_account, to_account, from_amount, status)
                )
                conn.commit()
        return status

    def update_balance(self, amount, account_id, cur):
        cur.execute(
            "UPDATE accounts SET balance = %s WHERE user_id = %s",
            (amount, account_id)
        )

    def get_account_balance(self, account_id):
        with psycopg.connect(CONNECTION_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT balance FROM accounts WHERE user_id = %s",
                    (account_id,)
                )
                return cur.fetchone()[0]


def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_CONNECTION_STRING],
        api_version=(0, 11, 5)
    )
    consumer = KafkaConsumer(
        'transactions',
        enable_auto_commit=True,
        bootstrap_servers=[KAFKA_CONNECTION_STRING]
    )
    service = MoneyTransferService()

    for message in consumer:
        transaction_data = json.loads(message.value.decode('utf-8'))
        print(transaction_data)
        status = service.transfer(**transaction_data)
        result = json.dumps({
            "transaction_id": transaction_data['transaction_id'],
            "status": status,
        }).encode("utf-8")
        producer.send("notification", result)


if __name__ == "__main__":
    main()
