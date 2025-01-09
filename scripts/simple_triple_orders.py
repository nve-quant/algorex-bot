from decimal import Decimal
from typing import Dict

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel, ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class TripleOrderConfig(BaseClientModel):
    exchange: str = Field(
        "binance_paper_trade",
        client_data=ClientFieldData(prompt=lambda mi: "Enter the exchange name"),
    )
    trading_pair: str = Field(
        "ETH-USDT",
        client_data=ClientFieldData(prompt=lambda mi: "Enter the trading pair"),
    )
    order1_price: Decimal = Field(
        default=Decimal("1800"),
        client_data=ClientFieldData(prompt=lambda mi: "Enter price for first order"),
    )
    order1_amount: Decimal = Field(
        default=Decimal("0.1"),
        client_data=ClientFieldData(prompt=lambda mi: "Enter amount for first order"),
    )
    order2_price: Decimal = Field(
        default=Decimal("1750"),
        client_data=ClientFieldData(prompt=lambda mi: "Enter price for second order"),
    )
    order2_amount: Decimal = Field(
        default=Decimal("0.15"),
        client_data=ClientFieldData(prompt=lambda mi: "Enter amount for second order"),
    )
    order3_price: Decimal = Field(
        default=Decimal("1700"),
        client_data=ClientFieldData(prompt=lambda mi: "Enter price for third order"),
    )
    order3_amount: Decimal = Field(
        default=Decimal("0.2"),
        client_data=ClientFieldData(prompt=lambda mi: "Enter amount for third order"),
    )


class TripleOrder(ScriptStrategyBase):
    def __init__(self, connectors: Dict[str, ConnectorBase], config: TripleOrderConfig):
        super().__init__(connectors)
        self.config = config
        self.placed_orders = False

    @classmethod
    def init_markets(cls, config: TripleOrderConfig):
        cls.markets = {config.exchange: {config.trading_pair}}

    def on_tick(self):
        if self.placed_orders:
            return
        
        # Place the three orders
        self.buy(
            connector_name=self.config.exchange,
            trading_pair=self.config.trading_pair,
            amount=self.config.order1_amount,
            order_type=OrderType.LIMIT,
            price=self.config.order1_price
        )
        self.logger().info(f"Placed first buy order: {self.config.order1_amount} at {self.config.order1_price}")

        self.buy(
            connector_name=self.config.exchange,
            trading_pair=self.config.trading_pair,
            amount=self.config.order2_amount,
            order_type=OrderType.LIMIT,
            price=self.config.order2_price
        )
        self.logger().info(f"Placed second buy order: {self.config.order2_amount} at {self.config.order2_price}")

        self.buy(
            connector_name=self.config.exchange,
            trading_pair=self.config.trading_pair,
            amount=self.config.order3_amount,
            order_type=OrderType.LIMIT,
            price=self.config.order3_price
        )
        self.logger().info(f"Placed third buy order: {self.config.order3_amount} at {self.config.order3_price}")

        self.placed_orders = True

    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        
        lines = []
        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])
        return "\n".join(lines) 