import os


KAFKA_BROKER_HOST = os.getenv("KAFKA_BROKER_HOST", "localhost")
KAFKA_BROKER_PORT = os.getenv("KAFKA_BROKER_PORT", 9092)
KAFKA_CONNECTION_STRING = f"{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", 5433)
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin")
DB_NAME = os.getenv("POSTGRES_DB", "maindb")

CONNECTION_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
