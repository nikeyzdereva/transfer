from contextlib import asynccontextmanager
from fastapi import FastAPI

from kafka import KafkaConsumer

from central_service import CentralService
from config import KAFKA_CONNECTION_STRING
from database import (
    create_account_table, create_transactions_table,
    create_users_table, add_test_data
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        create_account_table()
        create_transactions_table()
        create_users_table()
        add_test_data()
    except Exception:
        pass
    yield

app = FastAPI(title="Central service", lifespan=lifespan)


@app.get("/init_transfer")
def init_transfer(from_account, to_account, amount):
    service = CentralService(KAFKA_CONNECTION_STRING)
    transfer_id = service.init_transfer(
        from_account,
        to_account,
        amount,
    )

    return {
        "transfer_id": transfer_id,
    }


@app.get("/accounts")
def accounts():
    service = CentralService(KAFKA_CONNECTION_STRING)
    data = service.get_accounts()
    accounts = []
    for i in data:
        accounts.append({
            "id": i[0],
            "balance": i[1],
            "currency": i[2]
        })

    return {
        "accounts": accounts,
    }
