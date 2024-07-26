import requests, os, dotenv
from utils import generate_signature
import pandas as pd
from sqlalchemy.types import *
from db import DB_CONNECTION


def fetch_bot(bot_id: int):
    api_key = os.environ.get("commas_api_key")
    secret = os.environ.get("commas_secret")
    base_endpoint = "https://api.3commas.io"
    endpoint = f"/public/api/ver1/bots"
    signature = generate_signature(endpoint=endpoint, secret=secret)
    url = base_endpoint + endpoint
    headers = {"Apikey": api_key, "Signature": signature}
    response = requests.get(url=url, headers=headers)
    bots = response.json()
    bot_info = [
        {
            "id": bot["id"],
            "pair": bot["pairs"],
            "name": bot["name"],
            "created_at": bot["created_at"],
            "updated_at": bot["updated_at"],
            "is_enabled": bot["is_enabled"],
            "take_profit": bot["take_profit"],
            "finished_deals_count": bot["finished_deals_count"],
            "finished_deals_profit_usd": bot["finished_deals_profit_usd"],
            "active_deals_usd_profit": bot["active_deals_usd_profit"],
            "max_safety_orders": bot["max_safety_orders"],
            "base_order_volume": bot["base_order_volume"],
            "safety_order_volume": bot["safety_order_volume"],
        }
        for bot in bots
        if bot["id"] == bot_id
    ]

    print(bot_info)


def fetch_bots():
    api_key = os.environ.get("commas_api_key")
    secret = os.environ.get("commas_secret")
    base_endpoint = "https://api.3commas.io"
    endpoint = "/public/api/ver1/bots"
    url = base_endpoint + endpoint
    signature = generate_signature(endpoint=endpoint, secret=secret)
    headers = {"Apikey": api_key, "Signature": signature}
    rq = requests.get(url=url, headers=headers)
    bots = rq.json()
    return bots


def calculate_max_allocation_per_bot(
    max_safety_orders: int, bo_volume: float, so_volume: float
):
    max_allocation = (max_safety_orders * so_volume) + bo_volume
    return max_allocation


def get_bots_info():
    columns = [
        "id",
        "name",
        "pairs",
        "created_at",
        "updated_at",
        "max_safety_orders",
        "is_enabled",
        "take_profit",
        "finished_deals_count",
        "finished_deals_profit_usd",
        "active_deals_usd_profit",
        "base_order_volume",
        "safety_order_volume",
        "safety_order_step_percentage",
        "martingale_step_coefficient",
        "martingale_volume_coefficient",
        "cooldown",
    ]
    bots = fetch_bots()
    df = pd.DataFrame.from_dict(bots)[columns]
    bots_df = pd.DataFrame()
    bots_df["id"] = df["id"].astype("Int64")
    bots_df["name"] = df["name"].astype("str")
    bots_df["pairs"] = df["pairs"].astype("str")
    bots_df["created_at"] = pd.to_datetime(df["created_at"].astype("object"))
    bots_df["updated_at"] = pd.to_datetime(df["updated_at"].astype("object"))
    bots_df["max_safety_orders"] = df["max_safety_orders"].astype("object")
    bots_df["is_enabled"] = df["is_enabled"].astype("boolean")
    bots_df["take_profit"] = df["take_profit"].astype("Float64")
    bots_df["finished_deals_count"] = df["finished_deals_count"].astype("Float64")
    bots_df["finished_deals_profit_usd"] = df["finished_deals_profit_usd"].astype(
        "Float64"
    )
    bots_df["active_deals_usd_profit"] = df["active_deals_usd_profit"].astype("Float64")
    bots_df["base_order_volume"] = df["base_order_volume"].astype("Float64")
    bots_df["safety_order_volume"] = df["safety_order_volume"].astype("Float64")
    bots_df["volume_allocated"] = calculate_max_allocation_per_bot(
        max_safety_orders=bots_df["max_safety_orders"],
        bo_volume=bots_df["base_order_volume"],
        so_volume=bots_df["safety_order_volume"],
    )
    bots_df["roi_%"] = (
        bots_df["finished_deals_profit_usd"] / bots_df["volume_allocated"]
    ) * 100
    bots_df["safety_order_step_percentage"] = df["safety_order_step_percentage"].astype(
        "Float64"
    )
    bots_df["so_step_coefficient"] = df["martingale_step_coefficient"].astype("Float64")
    bots_df["so_volume_coefficient"] = df["martingale_volume_coefficient"].astype(
        "Float64"
    )
    bots_df["cooldown"] = df["cooldown"].astype("Int64")
    bots_df = bots_df.sort_values(by="finished_deals_profit_usd", ascending=False)

    table_name = "bots"
    connection = DB_CONNECTION()

    db_schema = {
        "id": Integer,
        "name": String(64),
        "pairs": String(64),
        "created_at": DateTime,
        "updated_at": DateTime,
        "max_safety_orders": Integer,
        "is_enabled": BOOLEAN,
        "take_profit": Float,
        "finished_deals_count": Integer,
        "finished_deals_profit_usd": Float,
        "active_deals_usd_profit": Float,
        "base_order_volume": Float,
        "safety_order_volume": Float,
        "safety_order_step_percentage": Float,
        "so_step_coefficient": Float,
        "so_volume_coefficient": Float,
        "cooldown": Integer,
        "volume_allocated": Integer,
    }
    bots_df.to_sql(
        con=connection.engine,
        name=table_name,
        if_exists="replace",
        index=False,
        dtype=db_schema,
    )
    connection.set_index(table_name=table_name, column_name="id")
    print("Bot data exported")
