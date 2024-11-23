import uuid
import psycopg

from config import CONNECTION_URL


# создаем таблицу пользователей
def create_users_table():
    with psycopg.connect(CONNECTION_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE users (
                    id UUID PRIMARY KEY,
                    name TEXT)
                """)
            conn.commit()


# создаем таблицу транзакций
def create_transactions_table():
    with psycopg.connect(CONNECTION_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id UUID PRIMARY KEY,
                    from_id UUID,
                    to_id UUID,
                    amount REAL,
                    status TEXT)
            """)
            conn.commit()


# создаем таблицу счетов
def create_account_table():
    with psycopg.connect(CONNECTION_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id SERIAL PRIMARY KEY,
                    user_id UUID,
                    currency TEXT,
                    balance REAL)
            """)
            conn.commit()


def add_test_data():
    with psycopg.connect(CONNECTION_URL) as conn:
        with conn.cursor() as cur:
            first = uuid.uuid4()
            second = uuid.uuid4()
            third = uuid.uuid4()
            cur.executemany(
                "INSERT INTO users (id, name) VALUES (%s, %s)",
                [
                    (first, "Петя"),
                    (second, "Катя"),
                    (third, "Вася"),
                ]
            )
            cur.executemany(
                "INSERT INTO accounts (user_id, currency, balance) VALUES (%s, %s, %s)",
                [
                    (first, "EUR", 1000.0),
                    (second, "RUB", 12345.0),
                    (third, "USD", 0.0),
                ]
            )

            conn.commit()
