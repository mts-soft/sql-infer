from datetime import datetime
from decimal import Decimal
from json import JSONDecodeError
import requests

SERVER = "http://0.0.0.0:8001"


def check_query(query: str):
    res = requests.post(
        f"{SERVER}/",
        json={"query": query},
    )
    try:
        print(res.json())
    except JSONDecodeError:
        print(res.status_code)
        print(res.text)


def execute_query(query: str, params: list[str | bool | int | Decimal | datetime]):
    res = requests.post(
        f"{SERVER}/run",
        json={"query": query, "params": params},
    )
    try:
        print(res.json())
    except JSONDecodeError:
        print(res.status_code)
        print(res.text)


check_query("select users.name as u, analytics.name as a from users join analytics on true")