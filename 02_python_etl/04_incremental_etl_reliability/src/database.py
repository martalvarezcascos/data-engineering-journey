import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_ROOT / ".env")


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "de_week3",
    "user": "de_user",
    "password": os.getenv("POSTGRES_PASSWORD"),
}

