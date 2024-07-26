import hmac, hashlib, requests, os, json, pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import *
import os, dotenv

dotenv.load_dotenv()

host = os.environ.get("host")
port = os.environ.get("port")
database = os.environ.get("database")
username = os.environ.get("db_username")
password = os.environ.get("db_password")


class DB_CONNECTION:
    def __init__(self) -> None:
        ssl_args = {"ssl_ca": "ca.pem"}
        self.engine = create_engine(
            url=f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}",
            connect_args=ssl_args,
        )
        pass

    def get_table_columns(self, table_name: str):
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name=table_name)
        return columns

    def connect(self):
        return self.engine

    def list_tables(self):
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        return tables

    def get_table(self, table_name: str):
        with self.engine.connect() as connection:
            try:
                db_response = connection.execute(text(f"SELECT * FROM {table_name}"))
                for raw in db_response:
                    print(raw)
                return db_response
            except SQLAlchemyError as e:
                error = e.__dict__["orig"]
                print(error)
            connection.close()

    def create_table(self, table_name: str):
        with self.engine.connect() as connection:
            try:
                table = connection.execute(
                    text(
                        f"""CREATE TABLE {table_name}(
                        id int,
                        name varchar(20),
                        pairs varchar(20),
                        created_at date,
                        updated_at date,
                        max_safety_orders int,
                        is_enabled bool,
                        take_profit int,
                        finished_deals_count int,
                        finished_deals_profit_usd float,
                        active_deals_usd_profit float,
                        base_order_volume float,
                        safety_order_volume float,
                        volume_allocated float,
                        roi float,
                        safety_order_step_percentage float,
                        so_step_coefficient float,
                        so_volume_coefficient float,
                        cooldown float,
                        PRIMARY KEY (id)
                        );"""
                    )
                )
                connection.close()
                return f"Table {table_name} created"
            except SQLAlchemyError as e:
                error = e.__dict__["orig"]
                return error

    def drop_table(self, table_name: str):
        with self.engine.connect() as connection:
            try:
                db_response = connection.execute(text(f"DROP TABLE {table_name}"))
                connection.close()
                return f"Table {table_name} deleted"
            except SQLAlchemyError as e:
                error = e.__dict__["orig"]
                return error
        return db_response

    def set_index(self, table_name: str, column_name: str):
        with self.engine.connect() as connection:
            try:
                connection.execute(
                    text(f"ALTER TABLE {table_name} ADD PRIMARY KEY (id)")
                )
                connection.close()
                print(
                    f"Primary index set for column -- {column_name} of table --{table_name}."
                )
            except SQLAlchemyError as e:
                error = e.__dict__["orig"]
                return error
