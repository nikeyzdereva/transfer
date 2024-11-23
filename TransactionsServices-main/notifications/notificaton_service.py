import json

from kafka import KafkaProducer, KafkaConsumer

from config import KAFKA_CONNECTION_STRING


# класс сервиса
class NotificatonService:
    def __init__(self, kafka_connection_string):
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_connection_string],
            api_version=(0, 11, 5)
        )

    def send_notification(self, data):
        status = data["status"]
        key = data["transaction_id"]
        if status == "ERROR":
            value = "Ошибка при совершении транзакции!"
        else:
            value = "Платеж прошел успешно!"
        print(value)
        self.producer.send("result", value.encode("utf-8"), key=str(key).encode("utf-8"))


def main():
    consumer = KafkaConsumer(
        'notification',
        enable_auto_commit=True,
        bootstrap_servers=[KAFKA_CONNECTION_STRING]
    )
    service = NotificatonService(KAFKA_CONNECTION_STRING)

    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        print(data)
        service.send_notification(data)


if __name__ == "__main__":
    main()
