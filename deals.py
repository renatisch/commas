import requests, os, dotenv
from utils import generate_signature
import pandas as pd
from sqlalchemy.types import *
from db import DB_CONNECTION


def fetch_deals():
    api_key = os.environ.get("commas_api_key")
    secret = os.environ.get("commas_secret")
    base_endpoint = "https://api.3commas.io"
    endpoint = "/public/api/ver1/deals?limit=1000"
    signature = generate_signature(endpoint=endpoint, secret=secret)
    url = base_endpoint + endpoint
    headers = {"Apikey": api_key, "Signature": signature}
    response = requests.get(url=url, headers=headers)
    deals = response.json()
    return deals


def get_deals_info():
    deals = fetch_deals()
    columns = [
        "id",
        "bot_id",
        "created_at",
        "updated_at",
        "closed_at",
        "type",
        "from_currency",
        "to_currency",
        "pair",
        "status",
        "take_profit",
        "final_profit",
        "final_profit_percentage",
        "usd_final_profit",
        "actual_profit",
        "actual_usd_profit",
        "deal_has_error",
        "bot_name",
    ]
    deals_df = pd.DataFrame.from_dict(deals)[columns]
    deals_df["id"] = deals_df["id"].astype("Int64")
    deals_df["bot_id"] = deals_df["bot_id"].astype("Int64")
    deals_df["created_at"] = pd.to_datetime(deals_df["created_at"].astype("object"))
    deals_df["updated_at"] = pd.to_datetime(deals_df["updated_at"].astype("object"))
    deals_df["closed_at"] = pd.to_datetime(deals_df["closed_at"].astype("object"))
    deals_df["type"] = deals_df["type"].astype("str")
    deals_df["from_currency"] = deals_df["from_currency"].astype("str")
    deals_df["to_currency"] = deals_df["to_currency"].astype("str")
    deals_df["pair"] = deals_df["pair"].astype("str")
    deals_df["status"] = deals_df["status"].astype("str")
    deals_df["take_profit"] = deals_df["take_profit"].astype("Float64")
    deals_df["final_profit"] = deals_df["final_profit"].astype("Float64")
    deals_df["final_profit_percentage"] = deals_df["final_profit_percentage"].astype(
        "Float64"
    )
    deals_df["usd_final_profit"] = deals_df["usd_final_profit"].astype("Float64")
    deals_df["actual_profit"] = deals_df["actual_profit"].astype("Float64")
    deals_df["actual_usd_profit"] = deals_df["actual_usd_profit"].astype("Float64")
    deals_df["deal_has_error"] = deals_df["deal_has_error"].astype("boolean")
    deals_df = deals_df[deals_df["status"] == "completed"]

    db_schema = {
        "id": BigInteger,
        "bot_id": Integer,
        "created_at": DateTime,
        "updated_at": DateTime,
        "closed_at": DateTime,
        "type": String(64),
        "from_currency": String(64),
        "to_currency": String(64),
        "pair": String(64),
        "status": String(64),
        "take_profit": Float,
        "final_profit": Float,
        "final_profit_percentage": Float,
        "usd_final_profit": Float,
        "actual_profit": Float,
        "actual_usd_profit": Float,
        "deal_has_error": BOOLEAN,
    }
    table_name = "deals"
    connection = DB_CONNECTION()

    deals_df.to_sql(
        con=connection.engine,
        name=table_name,
        if_exists="replace",
        index=False,
        dtype=db_schema,
    )
    connection.set_index(table_name=table_name, column_name="id")
    print("Deals data exported")
