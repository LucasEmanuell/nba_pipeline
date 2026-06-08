import pytest
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()


@pytest.fixture(scope="session")
def engine():
    url = os.getenv("DB_URL_EXTERNAL")
    if not url:
        pytest.skip("DB_URL_EXTERNAL não configurado")
    eng = create_engine(url)
    # verifica conexão antes de rodar qualquer teste
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))
    return eng


@pytest.fixture(scope="session")
def conn(engine):
    with engine.connect() as c:
        yield c
