import os
import time
import random
import asyncio
import sys
import math
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Dict, List, Any, Tuple

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
                        spread = best_ask_price - best_bid_price
                        if execution_price >= best_ask_price:
                            execution_price = best_ask_price - (spread * Decimal("0.05"))  # 5% inside spread
                        elif execution_price <= best_bid_price:
                            execution_price = best_bid_price + (spread * Decimal("0.05"))  # 5% inside spread
                        
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
        try:
            performance = await self.retrieve_performance_metrics()

            if performance is None:
                self.logger().warning("Unable to retrieve performance metrics")
                return False

            if self.config.max_loss > Decimal(0):
                # Use trade_pnl instead of total_pnl to exclude fees from shutdown logic
                trade_pnl = performance.trade_pnl
                best_ask_price = self.connectors[self.config.maker_exchange].get_price(
                    self.config.maker_pair, True
                )
                if best_ask_price is None:
                    self.logger().warning("Unable to fetch price for PnL calculation")
                    return False

                value = self.initial_base * best_ask_price + self.initial_quote
                pnl_percentage = (-trade_pnl) / value * 100

                self.logger().info(f"Current PnL: {pnl_percentage:.2f}% (Max allowed loss: {self.config.max_loss}%)")

                if -pnl_percentage <= -self.config.max_loss:
                    self.logger().warning(
                        f"Max loss of {self.config.max_loss}% exceeded with Pct: {pnl_percentage}% (excluding fees). Stopping strategy."
                    )
                    safe_ensure_future(self.stop_hummingbot())
                    return True

            if self.config.volume > Decimal(0):
                total_volume = performance.s_vol_quote
                self.logger().info(f"Current volume: ${total_volume} (Target: ${self.config.volume})")
                
                if total_volume > self.config.volume:
                    self.logger().warning(
                        f"Target volume of ${self.config.volume} reached. Current volume: ${total_volume}. Stopping strategy."
                    )
                    safe_ensure_future(self.stop_hummingbot())
                    return True

            return False
        except Exception as e:
            self.logger().error(f"Error in check_performance_and_stop: {str(e)}")
            return False

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
        if not self.strategy_active:
            self.logger().warning("Strategy not active - stopping tick execution")
            return

        if self.shutting_down:
            self.logger().warning("Strategy is shutting down - stopping tick execution")
            return

        if not self.do_tick:
            self.logger().warning("do_tick is False - stopping tick execution")
            return

        if self.last_cancel or self.first_cancle:
            self.logger().warning("Last cancel or first cancel flag set - initiating shutdown")
            safe_ensure_future(self.stop_hummingbot())
            return

        current_time = time.time()

        # Check if the time limit has been exceeded
        if self.config.time_limit > 0 and self.check_time_limit():
            self.logger().warning(f"Time limit of {self.config.time_limit} minutes exceeded - initiating shutdown")
            return

        # Ensure minimum time between orders
        if current_time - self.last_order_time < self.config.order_interval:
            self.logger().debug(f"Waiting for order interval ({self.config.order_interval}s) - {current_time - self.last_order_time}s elapsed")
            return

        # Only clean orders periodically
        if current_time - self.last_clean_time > self.config.order_interval * 10:
            self.logger().info("Periodic order cleanup triggered")
            self.last_clean_time = current_time
            safe_ensure_future(self.cancel_all_orders())
            return

        # Add balance check before trading
        if not safe_ensure_future(self.check_and_update_balances()):
            self.logger().warning("Balance check failed - skipping tick")
            return

        # Get current prices
        best_bid_price = self.connectors[self.config.maker_exchange].get_price(self.config.maker_pair, False)
        best_ask_price = self.connectors[self.config.maker_exchange].get_price(self.config.maker_pair, True)

        if best_bid_price is None or best_ask_price is None:
            self.logger().warning("Unable to fetch prices - skipping tick")
            return

        # Calculate amount within bounds
        amount = self.calculate_order_amount()
        if amount == Decimal("0"):
            self.logger().warning("Calculated order amount is zero - skipping tick")
            return

        # Calculate execution price within spread
        execution_price = self.calculate_price_within_spread(best_bid_price, best_ask_price)
        if execution_price is None:
            self.logger().warning("Could not calculate valid execution price - skipping tick")
            return

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
        try:
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
        except Exception as e:
            self.logger().error(f"Error in place_orders_and_update: {str(e)}")

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

    def get_prices_from_both_exchanges(self) -> Tuple[Decimal, Decimal, Decimal, Decimal]:
        """
        Get bid and ask prices from both exchanges.
        Returns (maker_bid, maker_ask, taker_bid, taker_ask)
        """
        maker_bid = self.connectors[self.config.maker_exchange].get_price(self.config.maker_pair, False)
        maker_ask = self.connectors[self.config.maker_exchange].get_price(self.config.maker_pair, True)
        taker_bid = self.connectors[self.config.taker_exchange].get_price(self.config.taker_pair, False)
        taker_ask = self.connectors[self.config.taker_exchange].get_price(self.config.taker_pair, True)
        
        return maker_bid, maker_ask, taker_bid, taker_ask

    def determine_market_phase(self, bid_price: Decimal, ask_price: Decimal):
        """
        Determine market phase based on orderbook dynamics with variable thresholds.
        """
        spread = ask_price - bid_price
        mid_price = (ask_price + bid_price) / Decimal("2")
        spread_percentage = (spread / mid_price) * Decimal("100")
        
        # Add some memory of recent price action
        if not hasattr(self, 'recent_spreads'):
            self.recent_spreads = []
        self.recent_spreads.append(spread_percentage)
        if len(self.recent_spreads) > 20:  # Keep last 20 spreads
            self.recent_spreads.pop(0)
        
        avg_spread = sum(self.recent_spreads) / len(self.recent_spreads)
        spread_volatility = Decimal(str(max(0.0001, 
            sum((s - avg_spread) ** 2 for s in self.recent_spreads) / len(self.recent_spreads))))
        
        # Dynamic thresholds based on recent spread behavior
        volatility_threshold = max(Decimal("0.3"), avg_spread * Decimal("1.5"))
        consolidation_threshold = min(Decimal("0.15"), avg_spread * Decimal("0.7"))
        
        # Determine if we should change phase
        if self.phase_candle_counter >= self.phase_duration:
            # Add randomness to phase duration
            base_duration = random.randint(8, 25)  # More variable base duration
            noise_factor = Decimal(str(random.uniform(0.8, 1.2)))  # ±20% noise
            
            if spread_percentage > volatility_threshold or spread_volatility > Decimal("0.01"):
                self.market_phase = 'volatility'
                self.phase_duration = max(3, int(base_duration * Decimal("0.4") * noise_factor))
            elif spread_percentage < consolidation_threshold and spread_volatility < Decimal("0.005"):
                self.market_phase = 'consolidation'
                self.phase_duration = max(5, int(base_duration * Decimal("1.2") * noise_factor))
            else:
                # More organic trend changes
                if self.market_phase in ['consolidation', 'volatility'] or self.market_phase is None:
                    # Higher chance of trend continuation after volatility
                    if self.market_phase == 'volatility' and random.random() < 0.7:
                        self.market_phase = 'trend_down' if spread_percentage > avg_spread else 'trend_up'
                    else:
                        self.market_phase = random.choice(['trend_up', 'trend_down'])
                else:
                    # Chance of trend continuation
                    if random.random() < 0.3:  # 30% chance to continue trend
                        pass  # Keep current trend
                    else:
                        self.market_phase = 'trend_down' if self.market_phase == 'trend_up' else 'trend_up'
                
                self.phase_duration = max(5, int(base_duration * noise_factor))
            
            self.phase_candle_counter = 0
            self.logger().info(
                f"Switching to {self.market_phase} phase for {self.phase_duration} ticks "
                f"(spread: {spread_percentage:.3f}%, avg: {avg_spread:.3f}%, vol: {spread_volatility:.5f})"
            )

    def calculate_price_within_spread(
        self, best_bid_price: Decimal, best_ask_price: Decimal
    ) -> Decimal:
        """
        Calculate execution price based on market phase with organic variations and spread stability protection.
        Also handles dust orders that attempt to manipulate the spread.
        """
        if best_bid_price is None or best_ask_price is None:
            self.logger().warning("Unable to fetch prices")
            return None
            
        spread = best_ask_price - best_bid_price
        mid_price = (best_ask_price + best_bid_price) / Decimal("2")

        # Get price increment early for use throughout the method
        try:
            price_increment = self.connectors[
                self.config.maker_exchange
            ].trading_rules[self.config.maker_pair].min_price_increment
        except Exception as e:
            self.logger().error(f"Error getting price increment: {str(e)}")
            return None
        
        # Check for dust orders trying to manipulate the spread
        base_asset, _ = self.config.maker_pair.split('-')
        
        # Get order book snapshots from both exchanges
        maker_orderbook = self.connectors[self.config.maker_exchange].get_order_book(self.config.maker_pair)
        taker_orderbook = self.connectors[self.config.taker_exchange].get_order_book(self.config.taker_pair)
        
        if maker_orderbook is None or taker_orderbook is None:
            self.logger().warning("Unable to fetch order books")
            return None

        # Define dust thresholds (in base token amount)
        DUST_THRESHOLD_MIN = Decimal("1")  # 5k tokens minimum to be considered dust
        DUST_THRESHOLD_MAX = Decimal("500")  # 20k tokens maximum to be considered dust
        
        # Check for dust orders at best bid/ask
        def is_dust_order(size: Decimal) -> bool:
            return DUST_THRESHOLD_MIN <= size <= DUST_THRESHOLD_MAX

        try:
            # Get sizes at best bid/ask using the correct method
            # Get the first bid and ask entries from the order book
            maker_bids = maker_orderbook.bid_entries()[:3]  # Get top 3 levels for validation
            maker_asks = maker_orderbook.ask_entries()[:3]
            taker_bids = taker_orderbook.bid_entries()[:3]
            taker_asks = taker_orderbook.ask_entries()[:3]
            
            if not maker_bids or not maker_asks or not taker_bids or not taker_asks:
                self.logger().warning("Order book is empty or insufficient depth")
                return None
                
            best_bid_size = Decimal(str(maker_bids[0].amount))
            best_ask_size = Decimal(str(maker_asks[0].amount))
            
            # Calculate total volume in top 3 levels for validation
            total_bid_volume = sum(Decimal(str(bid.amount)) for bid in maker_bids)
            total_ask_volume = sum(Decimal(str(ask.amount)) for ask in maker_asks)
            
            # Get prices from both exchanges for cross-validation
            maker_best_bid = Decimal(str(maker_bids[0].price))
            maker_best_ask = Decimal(str(maker_asks[0].price))
            taker_best_bid = Decimal(str(taker_bids[0].price))
            taker_best_ask = Decimal(str(taker_asks[0].price))
            
            # Track last traded price for position detection
            if not hasattr(self, 'last_traded_price'):
                self.last_traded_price = mid_price
            
            # Calculate price position relative to spread
            price_position = (self.last_traded_price - best_bid_price) / (best_ask_price - best_bid_price)
            
            # Define thresholds for "close to bid/ask"
            CLOSE_TO_THRESHOLD = Decimal("0.2")  # Consider price "close" if within 20% of bid/ask
            
            # Function to check if price is realistic
            def is_price_realistic(price: Decimal, side: str) -> bool:
                if side == "buy":
                    return (
                        abs(price - taker_best_ask) / taker_best_ask <= Decimal("0.005") and  # Within 0.5% of taker price
                        price <= maker_best_ask * Decimal("1.005")  # Not more than 0.5% above maker ask
                    )
                else:
                    return (
                        abs(price - taker_best_bid) / taker_best_bid <= Decimal("0.005") and  # Within 0.5% of taker price
                        price >= maker_best_bid * Decimal("0.995")  # Not more than 0.5% below maker bid
                    )

            # Track last dust order time to prevent too frequent attempts
            if not hasattr(self, 'last_dust_order_time'):
                self.last_dust_order_time = 0
            
            # Minimum time between dust order attempts (5 seconds)
            DUST_ORDER_COOLDOWN = 5
            current_time = time.time()
            
            # Check if enough time has passed since last dust order
            if current_time - self.last_dust_order_time < DUST_ORDER_COOLDOWN:
                return None

            # Enhanced dust opportunity detection with validation
            dust_opportunity = None
            
            # Only take dust sell orders when price is close to ask
            if (price_position >= (Decimal("1") - CLOSE_TO_THRESHOLD) and  # Price is close to ask
                is_dust_order(best_bid_size) and 
                spread > price_increment * Decimal("2") and
                total_bid_volume >= best_bid_size and  # Verify volume
                is_price_realistic(best_bid_price, "sell")):
                
                # Dust buy order close to ask - opportunity to sell
                dust_opportunity = ("sell", best_bid_price)
                self.logger().info(
                    f"Detected valid dust buy order near ask:\n"
                    f"Size: {best_bid_size} {base_asset}\n"
                    f"Price: {best_bid_price}\n"
                    f"Total depth: {total_bid_volume}\n"
                    f"Spread: {spread}\n"
                    f"Price position: {price_position}"
                )
                self.last_dust_order_time = current_time
                
            # Only take dust buy orders when price is close to bid
            elif (price_position <= CLOSE_TO_THRESHOLD and  # Price is close to bid
                  is_dust_order(best_ask_size) and 
                  spread > price_increment * Decimal("2") and
                  total_ask_volume >= best_ask_size and  # Verify volume
                  is_price_realistic(best_ask_price, "buy")):
                
                # Dust sell order close to bid - opportunity to buy
                dust_opportunity = ("buy", best_ask_price)
                self.logger().info(
                    f"Detected valid dust sell order near bid:\n"
                    f"Size: {best_ask_size} {base_asset}\n"
                    f"Price: {best_ask_price}\n"
                    f"Total depth: {total_ask_volume}\n"
                    f"Spread: {spread}\n"
                    f"Price position: {price_position}"
                )
                self.last_dust_order_time = current_time

            if dust_opportunity:
                side, price = dust_opportunity
                # Return the dust order price to take advantage of the opportunity
                return price
                
        except Exception as e:
            self.logger().error(f"Error checking for dust orders: {str(e)}")
            # Continue with normal pricing if dust check fails
            pass

        # If no dust opportunities, continue with normal spread-based pricing
        # Track recent prices for stability check
        if not hasattr(self, 'recent_prices'):
            self.recent_prices = []
        if not hasattr(self, 'recent_spreads'):
            self.recent_spreads = []
        if not hasattr(self, 'last_spread_update'):
            self.last_spread_update = time.time()
            
        current_time = time.time()
        # Only update spread history every 2 seconds
        if current_time - self.last_spread_update >= 2:
            self.recent_prices.append(mid_price)
            self.recent_spreads.append(spread / mid_price)
            self.last_spread_update = current_time
            
            # Keep last 5 spreads (representing 10 seconds of data)
            if len(self.recent_prices) > 5:
                self.recent_prices.pop(0)
            if len(self.recent_spreads) > 5:
                self.recent_spreads.pop(0)
            
        # Calculate average price and spread
        avg_price = sum(self.recent_prices) / len(self.recent_prices) if self.recent_prices else mid_price
        avg_spread_pct = sum(self.recent_spreads) / len(self.recent_spreads) if self.recent_spreads else (spread / mid_price)
        current_spread_pct = spread / mid_price
        
        # Check for abnormal spread conditions - more lenient now
        max_allowed_spread_multiple = Decimal("4")  # Allow up to 4x average spread
        max_price_deviation = Decimal("0.008")  # Allow up to 0.8% deviation from average price
        
        if current_spread_pct > (avg_spread_pct * max_allowed_spread_multiple):
            self.logger().warning(f"Abnormally wide spread detected: {current_spread_pct:.4%} vs avg {avg_spread_pct:.4%}")
            return None
            
        # Check for significant price deviations
        price_deviation = abs(mid_price - avg_price) / avg_price
        if price_deviation > max_price_deviation:
            self.logger().warning(f"Significant price deviation detected: {price_deviation:.4%}")
            return None
            
        # Additional protection against sudden bid/ask changes
        if len(self.recent_prices) > 1:
            last_price = self.recent_prices[-1]
            max_tick_move = Decimal("0.005")  # Allow up to 0.5% move per tick
            if abs(mid_price - last_price) / last_price > max_tick_move:
                self.logger().warning(f"Price movement too large in single tick: {abs(mid_price - last_price) / last_price:.4%}")
                return None

        # Update market phase and calculate phase progress
        self.determine_market_phase(best_bid_price, best_ask_price)
        raw_progress = Decimal(self.phase_candle_counter) / Decimal(self.phase_duration)
        phase_progress = Decimal(str(math.sin(float(raw_progress) * math.pi / 2)))  # Non-linear progression
        
        # Track price levels and their duration
        if not hasattr(self, 'current_price_level'):
            self.current_price_level = Decimal("0.5")
            self.level_duration = 0
            self.level_momentum = Decimal("0")
            self.last_direction = None
            self.consecutive_same_direction = 0
        
        # Determine if we should start a new price level
        should_change_level = False
        if self.level_duration > random.randint(3, 8):  # Variable duration at each level
            should_change_level = random.random() < 0.4  # 40% chance to change level after duration
        
        if should_change_level:
            # Determine new price level with more organic transitions
            if self.last_direction is None:
                # Initial direction with slight upward bias (55/45)
                self.last_direction = 1 if random.random() < 0.55 else -1
            else:
                # More natural direction changes based on current level and market phase
                if self.market_phase == 'consolidation':
                    # Higher chance of small movements during consolidation
                    continue_same = random.random() < 0.4
                    self.consecutive_same_direction = 1 if not continue_same else min(self.consecutive_same_direction + 1, 3)
                else:
                    # Adaptive continuation probability based on position in range
                    range_position = (self.current_price_level - Decimal("0.1")) / Decimal("0.8")  # Normalized position
                    # Lower continuation probability near edges
                    edge_factor = min(range_position, Decimal("1") - range_position) * Decimal("2")
                    continue_prob = float(Decimal("0.6") * edge_factor)
                    continue_same = random.random() < continue_prob
                    
                    if continue_same:
                        self.consecutive_same_direction += 1
                    else:
                        self.consecutive_same_direction = 1
                        self.last_direction = -self.last_direction
            
            # Calculate level change with more variation
            base_change = random.uniform(0.02, 0.15)  # Reduced from (0.05, 0.25)
            if random.random() < 0.1:  # Reduced chance from 0.15 to 0.1
                base_change *= random.uniform(1.2, 1.8)  # Reduced multiplier from (1.5, 2.5)
            
            level_change = Decimal(str(base_change)) * Decimal(str(self.last_direction))
            
            # Add market phase influence
            if self.market_phase == 'volatility':
                # Reduced amplification during volatility
                level_change *= Decimal(str(random.uniform(1.1, 1.4)))  # Reduced from (1.2, 1.8)
            elif self.market_phase == 'consolidation':
                # Increased dampening during consolidation
                level_change *= Decimal(str(random.uniform(0.2, 0.5)))  # Reduced from (0.3, 0.7)
            
            # Natural mean reversion when far from center
            center_pull = (Decimal("0.5") - self.current_price_level) * Decimal("0.1")  # Reduced from 0.15
            if abs(center_pull) > Decimal("0.08"):  # Reduced threshold from 0.1
                level_change += center_pull
            
            # Update price level with momentum and noise
            self.current_price_level += level_change + (self.level_momentum * Decimal("0.15"))  # Reduced from 0.2
            
            # Add random noise that persists
            noise_factor = Decimal(str(random.uniform(-0.03, 0.03)))  # Reduced from (-0.05, 0.05)
            if random.random() < 0.08:  # Reduced chance from 0.1
                noise_factor *= Decimal("1.5")  # Reduced multiplier from 2
            
            self.current_price_level += noise_factor
            
            # Ensure within bounds while allowing edge exploration
            self.current_price_level = max(Decimal("0.1"), min(Decimal("0.9"), self.current_price_level))
            
            # Update momentum with decay
            self.level_momentum = level_change * Decimal("0.5")  # Carry forward some momentum
            self.level_duration = 0
        else:
            # Small random walks during the same level
            micro_change = Decimal(str(random.uniform(-0.01, 0.01)))  # Reduced from (-0.02, 0.02)
            if random.random() < 0.03:  # Reduced chance from 0.05
                micro_change *= Decimal("1.5")  # Reduced multiplier from 2
            
            self.current_price_level += micro_change
            self.current_price_level = max(Decimal("0.1"), min(Decimal("0.9"), self.current_price_level))
            self.level_duration += 1
            
            # Gradual momentum decay
            self.level_momentum *= Decimal("0.95")
        
        # Calculate base factor with natural variation
        variation = Decimal(str(random.uniform(-0.02, 0.02)))  # Reduced from (-0.05, 0.05)
        if random.random() < 0.1:  # Reduced chance from 0.15
            variation *= Decimal(str(random.uniform(1.1, 1.3)))  # Reduced multiplier
        
        # Use last traded price as anchor to prevent large swings
        if not hasattr(self, 'last_execution_price'):
            self.last_execution_price = mid_price
        
        # Calculate base factor with stronger anchor to last price
        last_price_weight = Decimal(str(random.uniform(0.6, 0.8)))  # 60-80% weight to last price
        spread_position = (self.last_execution_price - best_bid_price) / spread
        
        # Ensure spread_position is within bounds
        spread_position = max(Decimal("0.1"), min(Decimal("0.9"), spread_position))
        
        # Add small random walk to spread position
        random_walk = Decimal(str(random.uniform(-0.03, 0.03)))  # Small random movement
        spread_position += random_walk
        
        # Blend between last position and new random position
        base_factor = (spread_position * last_price_weight) + (Decimal(str(random.uniform(0.3, 0.7))) * (Decimal("1") - last_price_weight))
        
        # Add very small noise for organic movement
        micro_noise = Decimal(str(random.uniform(-0.01, 0.01)))
        base_factor += micro_noise
        
        # Add market phase influence with reduced impact
        phase_influence = Decimal("0")
        if self.market_phase == 'trend_up':
            base_influence = random.uniform(0.01, 0.03)  # Reduced from (0.02, 0.08)
            if random.random() < 0.1:  # Reduced chance from 0.15
                base_influence *= random.uniform(1.1, 1.3)  # Reduced from (1.3, 1.6)
            phase_influence = Decimal(str(base_influence))
        elif self.market_phase == 'trend_down':
            base_influence = random.uniform(0.01, 0.03)  # Reduced from (0.02, 0.08)
            if random.random() < 0.1:  # Reduced chance from 0.15
                base_influence *= random.uniform(1.1, 1.3)  # Reduced from (1.3, 1.6)
            phase_influence = -Decimal(str(base_influence))
        elif self.market_phase == 'volatility':
            magnitude = random.uniform(0.02, 0.05)  # Reduced from (0.05, 0.15)
            if random.random() < 0.05:  # Reduced chance from 0.1
                magnitude *= random.uniform(1.1, 1.3)  # Reduced from (1.3, 1.6)
            phase_influence = Decimal(str(magnitude)) * (Decimal("1") if random.random() < 0.5 else Decimal("-1"))
        elif self.market_phase == 'consolidation':
            if random.random() < 0.05:  # Reduced chance from 0.08
                phase_influence = Decimal(str(random.uniform(0.01, 0.03)))  # Reduced from (0.03, 0.1)
                phase_influence *= (Decimal("1") if random.random() < 0.5 else Decimal("-1"))
            
        # Reduced momentum impact
        if not hasattr(self, 'price_momentum'):
            self.price_momentum = Decimal("0")
        
        # Calculate new momentum with more decay
        momentum_decay = Decimal(str(random.uniform(0.85, 0.92)))  # Increased decay
        self.price_momentum = self.price_momentum * momentum_decay
        
        # Add very small new momentum
        new_momentum = Decimal(str(random.uniform(-0.01, 0.01)))  # Reduced from (-0.02, 0.02)
        if random.random() < 0.05:  # Reduced chance from 0.1
            new_momentum *= Decimal("1.2")  # Reduced multiplier
        self.price_momentum += new_momentum
        
        # Apply reduced momentum to base factor
        base_factor += self.price_momentum * Decimal("0.3")  # Reduced impact
        
        # Ensure the final price doesn't move too far from last execution
        max_move = Decimal("0.003")  # Max 0.3% move from last price
        if abs(base_factor - spread_position) > max_move:
            # Limit the movement while preserving direction
            direction = Decimal("1") if base_factor > spread_position else Decimal("-1")
            base_factor = spread_position + (direction * max_move)
        
        # Final bounds check
        base_factor = max(Decimal("0.1"), min(Decimal("0.9"), base_factor))
        
        # Calculate final price within spread
        try:
            price_within_spread = best_bid_price + (spread * base_factor)
            
            # Ensure price respects minimum increment
            if price_increment:
                price_within_spread = (price_within_spread // price_increment) * price_increment
            
            # Additional protection against large moves
            max_price_change = self.last_execution_price * Decimal("0.003")  # Max 0.3% change
            price_within_spread = max(
                self.last_execution_price - max_price_change,
                min(self.last_execution_price + max_price_change, price_within_spread)
            )
            
            # Add drift
            if not hasattr(self, 'price_drift'):
                self.price_drift = Decimal("0")

            drift_change = Decimal(str(random.uniform(-0.0002, 0.0002)))  # Small random drift
            self.price_drift += drift_change
            max_drift = spread * Decimal("0.1")  # 10% of spread
            self.price_drift = max(min(self.price_drift, max_drift), -max_drift)

            # Apply drift to price_within_spread
            price_within_spread += (spread * self.price_drift)
            
            # Add level resistance
            if not hasattr(self, 'last_price_level'):
                self.last_price_level = None

            current_level = (price_within_spread - best_bid_price) / spread

            if self.last_price_level is not None:
                if abs(current_level - self.last_price_level) < Decimal("0.1"):
                    direction = Decimal("1") if random.random() > 0.5 else Decimal("-1")
                    price_within_spread += (spread * Decimal("0.05") * direction)

            self.last_price_level = current_level
            
            # Update last execution price
            self.last_execution_price = price_within_spread
            
            return price_within_spread
            
        except Exception as e:
            self.logger().error(f"Error in final price calculation: {str(e)}")
            return None

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

        # Add market phase info
        lines.extend([
            "\n  Market Phase Info:",
            f"    Current Phase: {self.market_phase}",
            f"    Phase Duration: {self.phase_duration} ticks",
            f"    Current Tick: {self.phase_candle_counter}"
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
                    self.logger().error(f"CRITICAL: Low {base_asset} balance on {exchange}: {self.balances[exchange]['base']}")
                    return False
                if self.balances[exchange]["quote"] < self.min_balance_threshold:
                    self.logger().error(f"CRITICAL: Low {quote_asset} balance on {exchange}: {self.balances[exchange]['quote']}")
                    return False

            self.last_balance_check = current_time
            return True

        except Exception as e:
            self.logger().error(f"CRITICAL: Error checking balances: {str(e)}")
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
                    # Cancel the unfilled matching order
                    if not order_info.get("sell_filled"):
                        self.logger().info(f"Cancelling unmatched buy order {pair_order_id} as sell order was filled by another counterparty")
                        safe_ensure_future(self.cancel(self.current_buy_exchange, self.config.maker_pair, pair_order_id))
                    elif not order_info.get("buy_filled"):
                        self.logger().info(f"Cancelling unmatched sell order {pair_order_id} as buy order was filled by another counterparty")
                        safe_ensure_future(self.cancel(self.current_sell_exchange, self.config.maker_pair, pair_order_id))
                    # Clean up tracking for both orders
                    del self.pending_order_pairs[order_id]
                    if pair_order_id in self.pending_order_pairs:
                        del self.pending_order_pairs[pair_order_id]
            else:
                # Paired order not found, must be filled with another counterparty
                self.volume_with_counterparties += fill_value
                del self.pending_order_pairs[order_id]
            
            self.total_volume_traded += fill_value
            
            # Log the fill and current statistics
            self.logger().info(
                f"Order filled - ID: {order_id}, Amount: {fill_amount}, Price: {fill_price}\n"
                f"Total Volume: ${self.total_volume_traded}\n"
                f"Matched Volume: ${self.matched_orders_volume}\n"
                f"Counterparty Volume: ${self.volume_with_counterparties}"
            )
