import os
from typing import cast

from sqlalchemy import Engine, create_engine
from sqlalchemy.engine import URL as DB_URL
from sqlalchemy.orm import Session as ASession, scoped_session, sessionmaker

from .model import Base


def build_db_url() -> DB_URL:
    return DB_URL.create(
        "postgresql+psycopg",
        username=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(cast(str, os.getenv("DB_PORT"))),
        database=os.getenv("DB_NAME"),
    )


def create_db_engine(*, pool_size: int = 20, max_overflow: int = 10) -> Engine:
    return create_engine(build_db_url(), pool_size=pool_size, max_overflow=max_overflow)


def create_session_factory(engine: Engine) -> scoped_session[ASession]:
    return scoped_session(sessionmaker(
        bind=engine,
        autoflush=False,
        expire_on_commit=False,
    ))


def init_schema(engine: Engine, *, drop_existing: bool = False) -> None:
    if drop_existing:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
