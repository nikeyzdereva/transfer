import asyncio
import json

from apscheduler.schedulers.background import BackgroundScheduler
from kafka import KafkaProducer, KafkaConsumer
from redis import Redis

from converter_service import ConverterService
from config import AVAIBLE_CURRENCY, API_KEY, REDIS_HOST, REDIS_PORT, KAFKA_CONNECTION_STRING


# задача обновления курсов в редисе
async def update_currency():
    converter = ConverterService(API_KEY)
    result = await converter.update_currency(AVAIBLE_CURRENCY)
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT)
    for first, other in result.items():
        for second, value in other.items():
            redis.set(f"{first}_{second}", value)


async def main():
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT)
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_CONNECTION_STRING],
        api_version=(0, 11, 5)
    )
    consumer = KafkaConsumer(
        'converts',
        enable_auto_commit=True,
        bootstrap_servers=[KAFKA_CONNECTION_STRING]
    )
    await update_currency()
    sheduler = BackgroundScheduler()
    sheduler.add_job(
        update_currency,
        trigger="interval",
        hours=4,
    )
    sheduler.start()

    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))

        print(data)
        temp = f"{data['from_currency']}_{data['to_currency']}"
        currency = float(redis.get(temp))
        result = json.dumps({
            "transaction_id": data['id'],
            "from_account": data['from'],
            "to_account": data['to'],
            "from_amount": data['amount'],
            "to_amount": currency * float(data['amount']),
        }).encode("utf-8")
        producer.send("transactions", result)


if __name__ == "__main__":
    asyncio.run(main())
