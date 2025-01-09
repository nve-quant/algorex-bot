import os
import time
import random
import asyncio
import sys
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Dict, List, Any

import pandas as pd
import requests
from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel, ClientFieldData
from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.client.performance import PerformanceMetrics
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import OrderType, TradeType


class SimpleVolConfig(BaseClientModel):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))

    maker_exchange: str = Field(
        "kucoin_paper_trade",
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "First exchange for maker orders"),
    )
    taker_exchange: str = Field(
        "binance_paper_trade",
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Second exchange for taker orders"),
    )
    maker_pair: str = Field(
        "ETH-USDT",
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Trading pair for maker orders"),
    )
    taker_pair: str = Field(
        "ETH-USDT",
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Trading pair for taker orders"),
    )
    base_dollar_amount: Decimal = Field(
        100,
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Base dollar amount for orders"),
    )
    max_dollar_amount: Decimal = Field(
        200,
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Maximum dollar amount for orders"),
    )
    order_interval: int = Field(
        60,
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Interval (seconds) between orders"),
    )
    displayed_volume: Decimal = Field(
        50000,
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Desired quote volume for execution"),
    )
    time_limit: int = Field(
        60,
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Bot runtime limit (minutes)"),
    )
    max_loss: Decimal = Field(
        200,
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Max allowed loss (%) before shutdown"),
    )
    volume: Decimal = Field(
        10000,
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Total volume to execute ($)"),
    )
    enter_exit: bool = Field(
        False,
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Buy needed coins to start and close exposure after? (True/False)",
        ),
    )


class SimpleVol(ScriptStrategyBase):
    """
    Volume-based bot that places buy and sell orders at the same price between bid and ask,
    using two accounts on different exchanges. It alternates between the two exchanges in each interval.
    The bot adjusts order sizes based on volatility and account balances.
    """

    def __init__(self, connectors: Dict[str, ConnectorBase], config: SimpleVolConfig):
        super().__init__(connectors)
        self.config = config
        self.current_buy_exchange = config.maker_exchange
        self.current_sell_exchange = config.taker_exchange
        self.switch_flag = True
        self.last_order_time = time.time()  # Initialize last_order_time
        self.last_clean_time = time.time()  # Initialize last_clean_time
        self.start_time = time.time()
        self.strategy_active = True
        self.order_placement_lock = asyncio.Lock()  # Add lock for order placement

        # Market structure tracking
        self.market_phase = 'trend_up'
        self.phase_candle_counter = 0
        self.phase_duration = random.randint(5, 15)
        self.initial_base = Decimal(0)
        self.initial_quote = Decimal(0)
        self.hummingbot_app = HummingbotApplication.main_application()
        self.cancel_interval = config.order_interval * 10
        self.do_tick = True
        self.shut_down = False
        self.first_cancle = False
        self.first_sale = False
        self.last_cancel = False

        # Balance tracking
        self.balance_tracker = {
            config.maker_exchange: Decimal("0"),
            config.taker_exchange: Decimal("0")
        }
        self.total_volume_traded = Decimal("0")
        self.volume_with_counterparties = Decimal("0")
        self.matched_orders_volume = Decimal("0")
        self.pending_order_pairs = {}  # Track matching order pairs

        # Shutdown process variables
        self.shutting_down = False
        self.shutdown_step = 0
        self.shutdown_round = 0
        self.max_shutdown_rounds = 5
        self.shutdown_lock = asyncio.Lock()

        # Add balance tracking
        self.last_balance_check = 0
        self.balance_check_interval = 30  # Check balances every 30 seconds
        self.min_balance_threshold = Decimal("0.001")  # Minimum balance to maintain
        self.balances = {
            config.maker_exchange: {"base": Decimal("0"), "quote": Decimal("0")},
            config.taker_exchange: {"base": Decimal("0"), "quote": Decimal("0")}
        }

    @classmethod
    def init_markets(cls, config: SimpleVolConfig):
        cls.markets = {
            config.maker_exchange: {config.maker_pair},
            config.taker_exchange: {config.taker_pair},
        }

    async def stop_hummingbot(self):
        """
        Initiate the shutdown procedure by setting the shutting_down flag.
        The actual steps are executed one at a time in the manage_shutdown method.
        """
        async with self.shutdown_lock:
            self.logger().info("Initiating shutdown procedure.")
            self.shutting_down = True
            self.do_tick = False  # Stop placing new orders

    async def manage_shutdown(self):
        """
        Manages the shutdown process, executing one action per tick.
        """
        async with self.shutdown_lock:
            await asyncio.sleep(3)
            if self.shutdown_step == 0:
                # Step 1: Cancel all orders
                await self.cancel_all_orders()
                self.logger().info(f"Shutdown round {self.shutdown_round + 1}/{self.max_shutdown_rounds}: Cancelled all orders.")
            elif self.shutdown_step == 1:
                # Step 2: Check balances
                base_asset, _ = self.config.maker_pair.split('-')
                exposure_eliminated = True
                for exchange in [self.current_buy_exchange, self.current_sell_exchange]:
                    base_balance = self.connectors[exchange].get_available_balance(base_asset)
                    if base_balance > Decimal('0.0001'):
                        exposure_eliminated = False
                if exposure_eliminated:
                    self.logger().info("No unwanted assets, exposure eliminated.")
                    # Stop Hummingbot
                    self.logger().info("Stopping Hummingbot application.")
                    hummingbot_app = HummingbotApplication.main_application()
                    hummingbot_app.stop()
                    sys.exit(0)
                else:
                    self.logger().info("Unwanted assets found, proceeding to sell them.")
            elif self.shutdown_step == 2:
                # Step 3: Sell unwanted assets
                base_asset, quote_asset = self.config.maker_pair.split('-')
                for exchange in [self.current_buy_exchange, self.current_sell_exchange]:
                    base_balance = self.connectors[exchange].get_available_balance(base_asset)
                    if base_balance > Decimal('0.0001'):
                        # Get the spread and ensure we have order book data
                        best_bid_price = self.connectors[exchange].get_price(self.config.maker_pair, False)
                        best_ask_price = self.connectors[exchange].get_price(self.config.maker_pair, True)
                        
                        if best_bid_price is None or best_ask_price is None:
                            self.logger().warning("Unable to fetch order book data. Skipping shutdown orders.")
                            continue
                            
                        # Use existing market phase logic to determine position in spread
                        execution_price = self.calculate_price_within_spread(best_bid_price, best_ask_price)
                        
                        # Double-check the price is actually inside the spread
                        if execution_price >= best_ask_price or execution_price <= best_bid_price:
                            self.logger().warning("Calculated price outside spread. Using mid-spread instead.")
                            execution_price = best_bid_price + ((best_ask_price - best_bid_price) * Decimal("0.5"))
                        
                        self.logger().info(f"Selling {base_balance} {base_asset} on {exchange} at price {execution_price}")
                        
                        # Ensure we have enough quote balance
                        required_quote = base_balance * execution_price
                        other_exchange = self.current_buy_exchange if exchange == self.current_sell_exchange else self.current_sell_exchange
                        available_quote = self.connectors[other_exchange].get_available_balance(quote_asset)
                        
                        if available_quote >= required_quote:
                            # Place coordinated limit orders inside spread
                            self.sell(exchange, self.config.maker_pair, base_balance, OrderType.LIMIT, execution_price)
                            self.buy(other_exchange, self.config.maker_pair, base_balance, OrderType.LIMIT, execution_price)
                            self.logger().info(f"Placed matching orders at {execution_price} for {base_balance} {base_asset}")
                        else:
                            self.logger().warning(f"Insufficient quote balance on {other_exchange} for matching order")
            elif self.shutdown_step == 3:
                # Step 4: Wait for orders to execute
                self.logger().info("Waiting for orders to execute.")

            # Increment the shutdown step
            self.shutdown_step += 1

            # Check if we need to start a new round
            if self.shutdown_step >= 4:
                self.shutdown_step = 0
                self.shutdown_round += 1
                if self.shutdown_round >= self.max_shutdown_rounds:
                    self.logger().info("Maximum shutdown rounds reached. Stopping Hummingbot.")
                    hummingbot_app = HummingbotApplication.main_application()
                    hummingbot_app.stop()
                    sys.exit(0)
                else:
                    self.logger().info(f"Starting shutdown round {self.shutdown_round + 1}/{self.max_shutdown_rounds}.")

    async def retrieve_performance_metrics(self):
        """
        Access the performance metrics directly from the trade database and balances.
        """
        try:
            hummingbot_app = HummingbotApplication.main_application()
            if hummingbot_app.strategy is None:
                self.logger().warning("No strategy is running.")
                return None

            with hummingbot_app.trade_fill_db.get_new_session() as session:
                trades = hummingbot_app._get_trades_from_session(
                    int(hummingbot_app.init_time * 1e3),
                    session=session,
                    config_file_path=hummingbot_app.strategy_file_name,
                )

            balances = await self.get_current_balances()

            if self.initial_quote == Decimal(0) or self.initial_base == Decimal(0):
                base_asset, quote_asset = self.config.maker_pair.split('-')
                for exchange, assets in balances.items():
                    self.initial_base += assets.get(base_asset, Decimal(0))
                    self.initial_quote += assets.get(quote_asset, Decimal(0))

            if trades and balances:
                performance_metrics = await PerformanceMetrics.create(
                    self.config.maker_pair, trades, balances
                )
                return performance_metrics
            else:
                self.logger().warning(
                    "No trades or balances available to calculate performance metrics."
                )
                return None

        except Exception as e:
            self.logger().error(f"Error retrieving performance metrics: {str(e)}")
            return None

    async def cancel_all_orders(self):
        """
        Cancels all active orders on both exchanges.
        """
        try:
            for exchange in [self.current_buy_exchange, self.current_sell_exchange]:
                try:
                    open_orders = self.get_active_orders(exchange)
                    for order in open_orders:
                        try:
                            await self.cancel(
                                connector_name=exchange,
                                trading_pair=order.trading_pair,
                                order_id=order.client_order_id
                            )
                        except Exception as e:
                            self.logger().error(f"Error cancelling order {order.client_order_id}: {str(e)}")
                    self.logger().info(f"All open orders successfully cancelled on {exchange}.")
                except Exception as e:
                    self.logger().error(f"Error cancelling orders on {exchange}: {str(e)}")
        except Exception as e:
            self.logger().error(f"Error in cancel_all_orders: {str(e)}")

    async def check_performance_and_stop(self):
        """
        Check the PnL and fees using performance metrics and stop the bot if max loss is exceeded. Also checks for volume.
        """
        performance = await self.retrieve_performance_metrics()

        if self.config.max_loss > Decimal(0) and performance is not None:
            total_pnl = performance.total_pnl
            best_ask_price = self.connectors[self.config.maker_exchange].get_price(
                self.config.maker_pair, True
            )
            value = self.initial_base * best_ask_price + self.initial_quote
            pnl_percentage = (-total_pnl) / value * 100

            if -pnl_percentage <= -self.config.max_loss:
                self.logger().info(
                    f"Max loss of {self.config.max_loss}% exceeded with Pct: {pnl_percentage}%. Stopping strategy."
                )
                safe_ensure_future(self.stop_hummingbot())
                return True

        if self.config.volume > Decimal(0) and performance is not None:
            total_volume = performance.s_vol_quote
            if total_volume > self.config.volume:
                self.logger().info(
                    f"Reached {self.config.volume} $ Volume. Stopping strategy."
                )
                safe_ensure_future(self.stop_hummingbot())
                return True

    def check_time_limit(self) -> bool:
        """
        Check if the bot has exceeded the time limit.
        """
        elapsed_time = time.time() - self.start_time  # Time in seconds
        if elapsed_time >= self.config.time_limit * 60:  # Convert time_limit to seconds
            self.logger().info("Time limit reached. Shutting down the strategy.")
            safe_ensure_future(self.stop_hummingbot())
            return True
        return False

    def on_tick(self):
        """
        Main logic of the bot that is executed every tick.
        """
        if not self.strategy_active or self.shutting_down or not self.do_tick:
            return

        if self.last_cancel or self.first_cancle:
            safe_ensure_future(self.stop_hummingbot())
            return

        current_time = time.time()

        # Check if the time limit has been exceeded
        if self.config.time_limit > 0 and self.check_time_limit():
            return

        # Ensure minimum time between orders
        if current_time - self.last_order_time < self.config.order_interval:
            self.logger().debug(f"Skipping tick, waiting for order interval ({self.config.order_interval}s)")
            return

        # Only clean orders periodically
        if current_time - self.last_clean_time > self.config.order_interval * 10:
            self.last_clean_time = current_time
            safe_ensure_future(self.cancel_all_orders())
            return

        # Add balance check before trading
        if not safe_ensure_future(self.check_and_update_balances()):
            self.logger().warning("Unable to verify balances, skipping tick")
            return

        # Schedule the performance check
        safe_ensure_future(self.check_performance_and_stop())

        # Get current prices
        best_bid_price = self.connectors[self.config.maker_exchange].get_price(self.config.maker_pair, False)
        best_ask_price = self.connectors[self.config.maker_exchange].get_price(self.config.maker_pair, True)

        if best_bid_price is None or best_ask_price is None:
            self.logger().warning("Unable to fetch prices. Skipping tick.")
            return

        # Calculate amount within bounds
        amount = self.calculate_order_amount()

        # Calculate execution price within spread
        execution_price = self.calculate_price_within_spread(best_bid_price, best_ask_price)

        # Place orders with validated amount
        safe_ensure_future(self.place_orders_and_update(
            self.current_sell_exchange,
            self.current_buy_exchange,
            amount,
            execution_price
        ))

    async def place_orders_and_update(self, sell_exchange: str, buy_exchange: str, 
                                    amount: Decimal, price: Decimal):
        """
        Places orders and handles the updates if successful
        """
        async with self.order_placement_lock:  # Use lock to prevent concurrent order placement
            current_time = time.time()
            if current_time - self.last_order_time < self.config.order_interval:
                self.logger().debug("Order placement attempted too soon, skipping")
                return

            success = await self.place_matching_orders(
                sell_exchange,
                buy_exchange,
                amount,
                price
            )

            if success:
                self.last_order_time = current_time  # Update last order time only on success
                safe_ensure_future(self.track_order_completion(amount))
                self.switch_exchanges()
            else:
                self.logger().warning("Failed to place matching orders, skipping trade")

    def calculate_random_order_size(self, mid_price: Decimal) -> Decimal:
        """
        Set order size to a random value between base_dollar_amount and max_dollar_amount.
        Converts the dollar amount to the base asset amount using the current price.
        """
        # Generate random dollar amount between base and max
        dollar_amount = Decimal(
            random.uniform(
                float(self.config.base_dollar_amount), 
                float(self.config.max_dollar_amount)
            )
        )
        
        # Convert dollar amount to base asset amount
        base_amount = dollar_amount / mid_price
        
        # Get the minimum trade size from trading rules
        min_size = self.connectors[self.config.maker_exchange].trading_rules[self.config.maker_pair].min_order_size
        
        # Round down to meet exchange requirements
        base_amount = Decimal(str(base_amount)).quantize(Decimal(str(min_size)), rounding=ROUND_DOWN)
        
        # ADDITIONAL SAFETY CHECKS
        dollar_value = base_amount * mid_price
        
        # Ensure we're within bounds even after rounding
        if dollar_value > self.config.max_dollar_amount:
            base_amount = (self.config.max_dollar_amount / mid_price).quantize(Decimal(str(min_size)), rounding=ROUND_DOWN)
        elif dollar_value < self.config.base_dollar_amount:
            base_amount = (self.config.base_dollar_amount / mid_price).quantize(Decimal(str(min_size)), rounding=ROUND_DOWN)
        
        # Final minimum size check
        if base_amount < min_size:
            base_amount = min_size
            if base_amount * mid_price > self.config.max_dollar_amount:
                self.logger().warning("Cannot place order: minimum size exceeds maximum dollar amount")
                return Decimal("0")
        
        return base_amount

    def calculate_price_within_spread(
        self, best_bid_price: Decimal, best_ask_price: Decimal
    ) -> Decimal:
        """
        Simulate natural market structures within the bid-ask spread.
        """
        spread = best_ask_price - best_bid_price
        price_increment = self.connectors[
            self.config.maker_exchange
        ].trading_rules[self.config.maker_pair].min_price_increment

        phase_progress = Decimal(self.phase_candle_counter) / Decimal(self.phase_duration)
        fluctuation = Decimal(random.uniform(-0.05, 0.05))

        if self.market_phase == 'trend_up':
            random_factor = Decimal(phase_progress) + fluctuation
            random_factor = max(Decimal(0.1), min(Decimal(0.9), random_factor))
            self.phase_candle_counter += 1
            if self.phase_candle_counter >= self.phase_duration:
                self.market_phase = random.choice(['consolidation', 'trend_down', 'volatility'])
                self.phase_candle_counter = 0
                self.phase_duration = random.randint(5, 15)
        elif self.market_phase == 'trend_down':
            random_factor = Decimal(1 - phase_progress) + fluctuation
            random_factor = max(Decimal(0.1), min(Decimal(0.9), random_factor))
            self.phase_candle_counter += 1
            if self.phase_candle_counter >= self.phase_duration:
                self.market_phase = random.choice(['consolidation', 'trend_up', 'volatility'])
                self.phase_candle_counter = 0
                self.phase_duration = random.randint(5, 15)
        elif self.market_phase == 'consolidation':
            random_factor = Decimal(random.uniform(0.4, 0.6))
            self.phase_candle_counter += 1
            if self.phase_candle_counter >= self.phase_duration:
                self.market_phase = random.choice(['trend_up', 'trend_down', 'volatility'])
                self.phase_candle_counter = 0
                self.phase_duration = random.randint(5, 15)
        elif self.market_phase == 'volatility':
            random_factor = Decimal(random.uniform(0.1, 0.9))
            self.phase_candle_counter += 1
            if self.phase_candle_counter >= 3:
                self.market_phase = random.choice(['trend_up', 'trend_down', 'consolidation'])
                self.phase_candle_counter = 0
                self.phase_duration = random.randint(5, 15)
        else:
            random_factor = Decimal(0.5)  # Default to mid-spread if phase is undefined

        price_within_spread = best_bid_price + spread * random_factor
        if price_within_spread <= best_bid_price:
            price_within_spread = best_bid_price + price_increment
        if price_within_spread >= best_ask_price:
            price_within_spread = best_ask_price - price_increment

        return price_within_spread

    def switch_exchanges(self):
        """
        Switch buy and sell exchanges after each tick and track cumulative balances.
        """
        if self.switch_flag:
            self.current_buy_exchange = self.config.taker_exchange
            self.current_sell_exchange = self.config.maker_exchange
        else:
            self.current_buy_exchange = self.config.maker_exchange
            self.current_sell_exchange = self.config.taker_exchange
        self.switch_flag = not self.switch_flag

    async def track_order_completion(self, order_amount: Decimal):
        """
        Track completed orders and update balance differences.
        """
        self.balance_tracker[self.current_sell_exchange] -= order_amount
        self.balance_tracker[self.current_buy_exchange] += order_amount
        order_value = order_amount * self.get_mid_price()
        self.total_volume_traded += order_value
        self.matched_orders_volume += order_value
        
        # Log current imbalances and volumes
        self.logger().info(
            f"Current balance differences - {self.config.maker_exchange}: {self.balance_tracker[self.config.maker_exchange]}, "
            f"{self.config.taker_exchange}: {self.balance_tracker[self.config.taker_exchange]}\n"
            f"Total Volume Traded: ${self.total_volume_traded}\n"
            f"Matched Orders Volume: ${self.matched_orders_volume}\n"
            f"Volume with Other Counterparties: ${self.volume_with_counterparties}"
        )

    def get_mid_price(self, trading_pair: str = None) -> Decimal:
        """Get current mid price for the trading pair."""
        if trading_pair is None:
            trading_pair = self.config.maker_pair
        best_bid = self.connectors[self.config.maker_exchange].get_price(trading_pair, False)
        best_ask = self.connectors[self.config.maker_exchange].get_price(trading_pair, True)
        if best_bid is None or best_ask is None:
            return Decimal("0")
        return (best_bid + best_ask) / Decimal("2")

    async def rebalance_accounts(self):
        """
        Rebalance accounts by trading the imbalance back.
        """
        async with self.order_placement_lock:  # Use lock for rebalancing too
            current_time = time.time()
            if current_time - self.last_order_time < self.config.order_interval:
                self.logger().debug("Rebalancing attempted too soon after last order, skipping")
                return

            base_asset, _ = self.config.maker_pair.split('-')
            maker_imbalance = self.balance_tracker[self.config.maker_exchange]

            if abs(maker_imbalance) > Decimal("0.0001"):
                best_bid = self.connectors[self.config.maker_exchange].get_price(self.config.maker_pair, False)
                best_ask = self.connectors[self.config.maker_exchange].get_price(self.config.maker_pair, True)
                
                if best_bid is None or best_ask is None:
                    self.logger().warning("Unable to fetch prices for rebalancing")
                    return
                    
                execution_price = self.calculate_price_within_spread(best_bid, best_ask)
                
                # Validate rebalancing order size
                if not self.validate_order_size(self.config.maker_exchange, abs(maker_imbalance), execution_price):
                    self.logger().warning("Rebalancing order size validation failed")
                    return
                
                if maker_imbalance > 0:
                    success = await self.place_matching_orders(
                        self.config.maker_exchange,
                        self.config.taker_exchange,
                        abs(maker_imbalance),
                        execution_price
                    )
                else:
                    success = await self.place_matching_orders(
                        self.config.taker_exchange,
                        self.config.maker_exchange,
                        abs(maker_imbalance),
                        execution_price
                    )
                
                if success:
                    self.last_order_time = current_time  # Update last order time on successful rebalance
                    self.balance_tracker = {
                        self.config.maker_exchange: Decimal("0"),
                        self.config.taker_exchange: Decimal("0")
                    }
                    self.logger().info(f"Rebalancing completed. Traded {abs(maker_imbalance)} at {execution_price}")

    def format_status(self) -> str:
        """
        Returns a more focused status of the current strategy.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."

        lines = []
        
        # Get trading pair components
        base_asset, quote_asset = self.config.maker_pair.split('-')
        
        # Only show balances for the active trading pair
        lines.append("\n  Active Pair Balances:")
        for exchange in [self.config.maker_exchange, self.config.taker_exchange]:
            base_balance = self.connectors[exchange].get_available_balance(base_asset)
            quote_balance = self.connectors[exchange].get_available_balance(quote_asset)
            lines.append(f"    {exchange}:")
            lines.append(f"      {base_asset}: {base_balance:.8f}")
            lines.append(f"      {quote_asset}: {quote_balance:.2f}")

        # Calculate and show imbalances
        maker_base = self.connectors[self.config.maker_exchange].get_available_balance(base_asset)
        taker_base = self.connectors[self.config.taker_exchange].get_available_balance(base_asset)
        base_imbalance = maker_base - taker_base
        
        maker_quote = self.connectors[self.config.maker_exchange].get_available_balance(quote_asset)
        taker_quote = self.connectors[self.config.taker_exchange].get_available_balance(quote_asset)
        quote_imbalance = maker_quote - taker_quote
        
        lines.extend([
            "\n  Account Imbalances:",
            f"    {base_asset}: {base_imbalance:+.8f}",
            f"    {quote_asset}: {quote_imbalance:+.2f}"
        ])

        # Display current buy/sell exchanges
        lines.extend([
            "\n  Active Exchanges:",
            f"    Buy: {self.current_buy_exchange}",
            f"    Sell: {self.current_sell_exchange}"
        ])

        # Add volume statistics with better formatting
        lines.extend([
            "\n  Volume Statistics:",
            f"    Total Volume Traded: ${self.total_volume_traded:.2f}",
            f"    Matched Orders Volume: ${self.matched_orders_volume:.2f}",
            f"    Volume with Other Counterparties: ${self.volume_with_counterparties:.2f}"
        ])

        return "\n".join(lines)

    async def get_current_balances(self) -> Dict[str, Dict[str, Decimal]]:
        """
        Fetch the current balances from the connectors.
        """
        balances = {}
        for exchange, connector in self.connectors.items():
            try:
                balances[exchange] = connector.get_all_balances()
            except Exception as e:
                self.logger().error(f"Error fetching balances from {exchange}: {str(e)}")
        return balances

    def get_balance_df(self):
        data = []
        for connector_name, connector in self.connectors.items():
            balances = connector.get_all_balances()
            for asset, total_balance in balances.items():
                available_balance = connector.get_available_balance(asset)
                data.append(
                    {
                        'Exchange': connector_name,
                        'Asset': asset,
                        'Total Balance': total_balance,
                        'Available Balance': available_balance,
                    }
                )
        return pd.DataFrame(data)

    async def create_batch_orders(self, exchange: str, orders_to_create: List[LimitOrder]) -> List[LimitOrder]:
        """
        Creates multiple orders in a single batch when possible.
        """
        # Validate all orders before attempting to create any
        for order in orders_to_create:
            if not self.validate_order_size(exchange, order.quantity, order.price):
                self.logger().error("Order size validation failed for batch order")
                return []
                
        market_pair = self._market_trading_pair_tuple(connector_name=exchange, trading_pair=self.config.maker_pair)
        market = market_pair.market
        
        if hasattr(market, "batch_order_create"):
            submitted_orders = market.batch_order_create(orders_to_create=orders_to_create)
            
            # Track the orders
            for order in submitted_orders:
                self.start_tracking_limit_order(
                    market_pair=market_pair,
                    order_id=order.client_order_id,
                    is_buy=order.is_buy,
                    price=order.price,
                    quantity=order.quantity,
                )
            return submitted_orders
        else:
            # Fallback to individual order creation if batch not supported
            results = []
            for order in orders_to_create:
                if order.is_buy:
                    order_id = await self.buy(
                        exchange,
                        self.config.maker_pair,
                        order.quantity,
                        OrderType.LIMIT,
                        order.price,
                    )
                else:
                    order_id = await self.sell(
                        exchange,
                        self.config.maker_pair,
                        order.quantity,
                        OrderType.LIMIT,
                        order.price,
                    )
                results.append(order_id)
            return results

    async def place_matching_orders(self, sell_exchange: str, buy_exchange: str, amount: Decimal, price: Decimal) -> bool:
        """
        Places matching limit orders simultaneously with amount validation.
        """
        try:
            # Validate order size for both exchanges
            if not self.validate_order_size(sell_exchange, amount, price):
                return False
            if not self.validate_order_size(buy_exchange, amount, price):
                return False

            base_asset, quote_asset = self.config.maker_pair.split('-')
            
            # Create order objects with unique IDs
            sell_order_id = self.generate_client_order_id("sell", self.config.maker_pair)
            buy_order_id = self.generate_client_order_id("buy", self.config.maker_pair)
            
            sell_order = LimitOrder(
                client_order_id=sell_order_id,
                trading_pair=self.config.maker_pair,
                is_buy=False,
                base_currency=base_asset,
                quote_currency=quote_asset,
                price=price,
                quantity=amount
            )
            
            buy_order = LimitOrder(
                client_order_id=buy_order_id,
                trading_pair=self.config.maker_pair,
                is_buy=True,
                base_currency=base_asset,
                quote_currency=quote_asset,
                price=price,
                quantity=amount
            )

            # Store the order pair for tracking
            order_value = amount * price
            self.pending_order_pairs[sell_order_id] = {
                "buy_order_id": buy_order_id,
                "amount": amount,
                "price": price,
                "value": order_value,
                "sell_filled": False,
                "buy_filled": False
            }
            self.pending_order_pairs[buy_order_id] = {
                "sell_order_id": sell_order_id,
                "amount": amount,
                "price": price,
                "value": order_value,
                "sell_filled": False,
                "buy_filled": False
            }

            # Try to create orders in batches if possible
            results = await asyncio.gather(
                self.create_batch_orders(sell_exchange, [sell_order]),
                self.create_batch_orders(buy_exchange, [buy_order]),
                return_exceptions=True
            )
            
            for result in results:
                if isinstance(result, Exception):
                    self.logger().error(f"Error placing orders: {str(result)}")
                    await self.cancel_all_orders()
                    return False
                
            self.logger().info(f"Successfully placed matching orders at {price} for {amount}")
            return True
            
        except Exception as e:
            self.logger().error(f"Error in place_matching_orders: {str(e)}")
            await self.cancel_all_orders()
            return False

    def generate_client_order_id(self, side: str, trading_pair: str) -> str:
        """Generate a unique client order ID."""
        return f"HBOT-{side[0].upper()}BGUT{get_tracking_nonce()}"

    async def check_and_update_balances(self) -> bool:
        """
        Check and update balances for both exchanges.
        Returns True if balances are sufficient, False otherwise.
        """
        current_time = time.time()
        if current_time - self.last_balance_check < self.balance_check_interval:
            return True  # Use cached balances if checked recently

        try:
            base_asset, quote_asset = self.config.maker_pair.split('-')
            
            for exchange in [self.config.maker_exchange, self.config.taker_exchange]:
                self.balances[exchange]["base"] = self.connectors[exchange].get_available_balance(base_asset)
                self.balances[exchange]["quote"] = self.connectors[exchange].get_available_balance(quote_asset)
                
                # Log current balances
                self.logger().info(f"Current balances for {exchange}:")
                self.logger().info(f"{base_asset}: {self.balances[exchange]['base']}")
                self.logger().info(f"{quote_asset}: {self.balances[exchange]['quote']}")
                
                # Check if balances are too low
                if self.balances[exchange]["base"] < self.min_balance_threshold:
                    self.logger().warning(f"Low {base_asset} balance on {exchange}: {self.balances[exchange]['base']}")
                if self.balances[exchange]["quote"] < self.min_balance_threshold:
                    self.logger().warning(f"Low {quote_asset} balance on {exchange}: {self.balances[exchange]['quote']}")

            self.last_balance_check = current_time
            return True

        except Exception as e:
            self.logger().error(f"Error checking balances: {str(e)}")
            return False

    def calculate_order_amount(self) -> Decimal:
        """
        Calculates a random order amount within configured bounds.
        """
        try:
            # Convert dollar amounts to token amounts using current price
            base_amount = self.config.base_dollar_amount
            max_amount = self.config.max_dollar_amount
            
            # Get current price
            mid_price = self.get_mid_price()
            
            if mid_price == Decimal("0"):
                self.logger().warning("Unable to fetch mid price, using minimum amount")
                return Decimal(str(self.config.base_dollar_amount))
            
            # Calculate token amounts
            base_tokens = Decimal(str(base_amount)) / Decimal(str(mid_price))
            max_tokens = Decimal(str(max_amount)) / Decimal(str(mid_price))
            
            # Generate random amount between base and max
            random_amount = Decimal(str(random.uniform(float(base_tokens), float(max_tokens))))
            
            # Round to appropriate decimal places (e.g., 2)
            rounded_amount = random_amount.quantize(Decimal("0.01"))
            
            # Add safety checks
            if rounded_amount > max_tokens:
                self.logger().warning(f"Amount {rounded_amount} exceeds max, using max instead")
                rounded_amount = max_tokens
            
            if rounded_amount < base_tokens:
                self.logger().warning(f"Amount {rounded_amount} below min, using min instead")
                rounded_amount = base_tokens
            
            self.logger().info(f"Calculated order amount: {rounded_amount} tokens (${rounded_amount * mid_price})")
            return rounded_amount
            
        except Exception as e:
            self.logger().error(f"Error calculating order amount: {str(e)}")
            return Decimal(str(self.config.base_dollar_amount))

    def validate_order_size(self, exchange: str, amount: Decimal, price: Decimal) -> bool:
        """
        Validates order size against exchange rules and configured limits.
        """
        try:
            trading_rules = self.connectors[exchange].trading_rules[self.config.maker_pair]
            
            # Check minimum order size
            if amount < trading_rules.min_order_size:
                self.logger().warning(f"Order size {amount} below exchange minimum {trading_rules.min_order_size}")
                return False
                
            # Check maximum order size
            if trading_rules.max_order_size is not None and amount > trading_rules.max_order_size:
                self.logger().warning(f"Order size {amount} above exchange maximum {trading_rules.max_order_size}")
                return False
                
            # Check minimum notional value
            notional = amount * price
            if trading_rules.min_notional_size is not None and notional < trading_rules.min_notional_size:
                self.logger().warning(f"Order notional ${notional} below exchange minimum ${trading_rules.min_notional_size}")
                return False
                
            # Check against our configured limits
            dollar_value = amount * price
            if dollar_value > self.config.max_dollar_amount:
                self.logger().warning(f"Order value ${dollar_value} exceeds max ${self.config.max_dollar_amount}")
                return False
                
            if dollar_value < self.config.base_dollar_amount:
                self.logger().warning(f"Order value ${dollar_value} below min ${self.config.base_dollar_amount}")
                return False
                
            return True
            
        except Exception as e:
            self.logger().error(f"Error validating order size: {str(e)}")
            return False

    def did_fill_order(self, event):
        """
        Callback when an order is filled. Updates volume tracking based on whether it was matched or not.
        """
        order_id = event.order_id
        if order_id in self.pending_order_pairs:
            fill_amount = event.amount
            fill_price = event.price
            fill_value = fill_amount * fill_price
            
            order_info = self.pending_order_pairs[order_id]
            
            if "buy_order_id" in order_info:  # This is a sell order
                order_info["sell_filled"] = True
                pair_order_id = order_info["buy_order_id"]
            else:  # This is a buy order
                order_info["buy_filled"] = True
                pair_order_id = order_info["sell_order_id"]
            
            pair_info = self.pending_order_pairs.get(pair_order_id)
            if pair_info:
                # If both orders are filled within a small time window, consider it matched
                if order_info.get("sell_filled") and order_info.get("buy_filled"):
                    self.matched_orders_volume += fill_value
                    # Clean up tracking
                    del self.pending_order_pairs[order_id]
                    del self.pending_order_pairs[pair_order_id]
                else:
                    # One side filled without the other, likely with another counterparty
                    self.volume_with_counterparties += fill_value
            else:
                # Paired order not found, must be filled with another counterparty
                self.volume_with_counterparties += fill_value
                del self.pending_order_pairs[order_id]
            
            self.total_volume_traded += fill_value
            
            # Log the fill
            self.logger().info(
                f"Order filled - ID: {order_id}, Amount: {fill_amount}, Price: {fill_price}\n"
                f"Total Volume: ${self.total_volume_traded}\n"
                f"Matched Volume: ${self.matched_orders_volume}\n"
                f"Counterparty Volume: ${self.volume_with_counterparties}"
            )
