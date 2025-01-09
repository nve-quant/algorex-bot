from decimal import Decimal
from typing import Any, Dict
import datetime

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.001"),
    buy_percent_fee_deducted_from_returns=True
)


def is_market_active(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return exchange_info.get("active", False) or exchange_info.get("isActive", False)


def convert_fromiso_to_unix_timestamp(date_str: str) -> int:
    date_object = datetime.datetime.fromisoformat(date_str.rstrip('Z'))
    return int(date_object.timestamp() * 1000)


class XtcomConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="xtcom", const=True, client_data=None)
    xtcom_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your XT.com API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    xtcom_api_secret: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your XT.com API secret",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "xtcom"


KEYS = XtcomConfigMap.construct()
