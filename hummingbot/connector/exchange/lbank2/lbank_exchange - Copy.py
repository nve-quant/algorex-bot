import asyncio
import hashlib
import hmac
import json
import math
import random
import string
import sys
import time
import urllib.parse
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

from async_timeout import timeout
from bidict import bidict
from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.connector.constants import s_decimal_0, s_decimal_NaN
from hummingbot.connector.exchange.lbank import lbank_constants as CONSTANTS, lbank_utils, lbank_web_utils as web_utils
from hummingbot.connector.exchange.lbank.lbank_api_order_book_data_source import LbankAPIOrderBookDataSource
from hummingbot.connector.exchange.lbank.lbank_api_user_stream_data_source import LbankAPIUserStreamDataSource
from hummingbot.connector.exchange.lbank.lbank_auth import LbankAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import TradeFillOrderDetails, combine_to_hb_trading_pair
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase, TradeFeeSchema
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, RESTResponse
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class LbankExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    @classmethod
    def name(cls) -> str:
        return "lbank"

    @classmethod
    def is_centralized(cls) -> bool:
        return True

    @classmethod
    def conf_path(cls) -> str:
        return "conf/connectors/lbank.yml"

    @classmethod
    def get_connector_config_map(cls) -> Dict:
        return {
            "lbank_api_key": ConfigVar(
                key="lbank_api_key",
                prompt="Enter your LBank API key >>> ",
                is_secure=True,
                is_connect_key=True,
                prompt_on_new=True,
            ),
            "lbank_secret_key": ConfigVar(
                key="lbank_secret_key", 
                prompt="Enter your LBank secret key >>> ",
                is_secure=True,
                is_connect_key=True,
                prompt_on_new=True,
            ),
            "lbank_auth_method": ConfigVar(
                key="lbank_auth_method",
                prompt="Enter your LBank auth method (RSA/HmacSHA256) >>> ",
                is_secure=False,
                is_connect_key=True,
                prompt_on_new=True,
                default="HmacSHA256"
            ),
        }

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 lbank_api_key: str,
                 lbank_secret_key: str,
                 lbank_auth_method: str = "HmacSHA256",
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        """Initialize the LBank exchange."""
        self._api_key = lbank_api_key
        self._secret_key = lbank_secret_key
        self._auth_method = lbank_auth_method
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        
        # Initialize the trading pair maps before calling super().__init__
        self._trading_pair_symbol_map = None
        self.symbol_trading_pair_map = {}
        
        super().__init__(client_config_map)
        
        # Initialize timestamps
        self._last_trades_poll_lbank_timestamp = 0
        self._last_poll_timestamp = 0
        
        # Initialize URL endpoints using correct constant names
        self._user_info_url = CONSTANTS.ACCOUNTS_PATH_URL
        self._trade_history_url = CONSTANTS.TRADE_UPDATES_PATH_URL
        self._user_trades_url = CONSTANTS.TRADE_UPDATES_PATH_URL
        
        # Initialize auth
        self._auth = LbankAuth(
            api_key=self._api_key,
            secret_key=self._secret_key,
            auth_method=self._auth_method,
        )
        
        # Initialize web assistants factory
        self._web_assistants_factory = web_utils.build_api_factory(
            auth=self._auth,
            domain=self._domain,
        )
        
        # Initialize trackers
        self._order_book_tracker = None
        self._user_stream_tracker = None
        self._account_balances = {}
        self._account_available_balances = {}
        
        # Initialize user stream tracker if trading is required
        if self._trading_required:
            self._user_stream_tracker = self._create_user_stream_tracker()

        # Add these properties
        self._last_order_update_timestamp = 0  # Track last order update
        self._order_refresh_time = 30.0  # Default refresh time

    def _create_user_stream_tracker(self) -> UserStreamTrackerDataSource:
        """
        Creates a new user stream tracker instance.
        """
        user_stream_tracker = LbankAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
        )
        return user_stream_tracker

    @staticmethod
    def lbank_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(lbank_type: str) -> OrderType:
        return OrderType[lbank_type]

    def _time_provider(self) -> int:
        """
        Provide current time (in ms) without referencing any unpickleable coroutine or lambda.
        """
        return int(self._time_synchronizer.time() * 1e3)

    @property
    def authenticator(self) -> LbankAuth:
        """
        Authenticator property to handle authentication for API requests
        """
        return LbankAuth(
            api_key=self._api_key,
            secret_key=self._secret_key,
            auth_method=self._auth_method,
            time_provider=self._time_provider
        )

    @property
    def name(self) -> str:
        return "lbank"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.GET_TIMESTAMP_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def account_balances(self) -> Dict[str, Decimal]:
        """
        Returns a dictionary of token balances with default decimal values
        """
        return {k: v if v is not None else s_decimal_0 
                for k, v in self._account_balances.items()}

    @property
    def account_available_balances(self) -> Dict[str, Decimal]:
        """
        Returns a dictionary of available token balances with default decimal values
        """
        return {k: v if v is not None else s_decimal_0 
                for k, v in self._account_available_balances.items()}

    def get_balance(self, currency: str) -> Decimal:
        """
        Gets balance for a specific currency, returns 0 if not found
        """
        balance = self._account_balances.get(currency, s_decimal_0)
        return balance if balance is not None else s_decimal_0

    def get_available_balance(self, currency: str) -> Decimal:
        """
        Gets available balance for a specific currency, returns 0 if not found
        """
        balance = self._account_available_balances.get(currency, s_decimal_0)
        return balance if balance is not None else s_decimal_0

    def get_all_balances(self) -> Dict[str, Decimal]:
        """
        Return a dictionary of all account balances
        Used by balance_command.py to get balances and calculate USD values using RateOracle
        """
        return self._account_balances.copy()

    def get_balance_display(self) -> str:
        """
        Helper method to format the balance display string
        """
        lines = []
        total_value = Decimal("0")
        
        # Header
        lines.append("lbank:")
        lines.append(f"    {'Asset':<6} {'Amount':>12} {'USD Value':>12} {'Allocated'}")
        
        # Asset rows
        for asset, balance in sorted(self._account_balances.items()):
            usd_value = self._account_balances_usd_value.get(asset, Decimal("0"))
            total_value += usd_value
            
            # Format with exact spacing to match client output
            balance_str = f"{balance:>12.4f}"
            usd_value_str = f"{usd_value:>12.2f}"
            
            lines.append(f"    {asset:<6} {balance_str} {usd_value_str} {'100%'}")
        
        # Add total value
        lines.append("")
        lines.append(f"Total: $ {total_value:.2f}")
        lines.append("Allocated: 0.00%")
        
        return "\n".join(lines)

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    async def get_all_pairs_prices(self) -> List[Dict[str, Any]]:
        """
        Gets ticker prices for all trading pairs from LBank
        :return: List of dictionaries containing ticker information
        """
        # LBank requires a symbol parameter even when getting all tickers
        response = await self._api_get(
            path_url=CONSTANTS.TICKER_BOOK_PATH_URL,
            params={"symbol": "all"}  # "all" returns data for all trading pairs
        )
        
        if not isinstance(response, list):
            raise Exception(f"Unexpected response format from ticker endpoint: {response}")
        
        return response

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = ("-1021" in error_description
                                        and "Timestamp for this request" in error_description)
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in str(
            status_update_exception
        ) and CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return str(CONSTANTS.UNKNOWN_ORDER_ERROR_CODE) in str(
            cancelation_exception
        ) and CONSTANTS.UNKNOWN_ORDER_MESSAGE in str(cancelation_exception)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        """
        Creates a user stream data source instance for user stream tracking.
        """
        return LbankAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self.trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory)

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = order_type is OrderType.LIMIT_MAKER
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    def get_lbank_order_type(self, trade_type: TradeType, order_type: OrderType) -> str:
        """
        Converts Hummingbot order types to LBank order types
        :param trade_type: The trade type (BUY/SELL)
        :param order_type: The order type (LIMIT/MARKET/LIMIT_MAKER)
        :return: LBank order type string
        """
        if order_type == OrderType.LIMIT_MAKER:
            # LBank uses buy_maker/sell_maker for post-only orders
            return "buy_maker" if trade_type == TradeType.BUY else "sell_maker"
        elif order_type == OrderType.LIMIT:
            return "buy" if trade_type == TradeType.BUY else "sell"
        elif order_type == OrderType.MARKET:
            return "buy_market" if trade_type == TradeType.BUY else "sell_market"
        else:
            raise ValueError(f"Unsupported order type: {order_type}")

    def _sort_params_by_name(self, params: dict) -> dict:
        """
        Sort parameters by comparing each letter of parameter names.
        If first letters are same, compare second letters, and so on.
        """
        return dict(sorted(params.items()))

    def _generate_order_id(self, side: str, trading_pair: str) -> str:
        """Generate a unique order ID based on timestamp and random string."""
        ts = int(time.time() * 1000)
        rand_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        return f"LB-{side}-{trading_pair}-{ts}-{rand_id}"

    async def _create_order(
            self,
            trade_type: TradeType,
            order_id: str,
            trading_pair: str,
            amount: Decimal,
            order_type: OrderType,
            price: Decimal = s_decimal_0,
            **kwargs) -> Dict[str, Any]:
        """
        Creates an order in the exchange using the parameters to configure it
        """
        try:
            exchange_trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair)
            
            self.logger().info(f"Creating order - Pair: {exchange_trading_pair}, Type: {trade_type}, Amount: {amount}, Price: {price}")
            
            # Determine order type
            type_str = "buy_maker" if trade_type == TradeType.BUY else "sell_maker"
            
            # Create base parameters
            params = {
                "symbol": exchange_trading_pair,
                "type": type_str,
                "price": str(price),
                "amount": str(amount),
                "custom_id": order_id
            }
            
            # Create REST request
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            request = RESTRequest(
                method=RESTMethod.POST,
                url=web_utils.private_rest_url(path_url=CONSTANTS.CREATE_ORDER_PATH_URL),
                data=params,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                is_auth_required=True
            )

            self.logger().info(f"Sending order request: {request}")
            raw_response = await rest_assistant.call(request)
            
            response = await raw_response.json()
            self.logger().info(f"Order response: {response}")
            
            if not response.get("result", False):
                error_msg = response.get("msg", "Unknown error")
                error_code = response.get("error_code", "Unknown code")
                self.logger().error(f"Order creation failed: {error_msg} (code: {error_code})")
                raise ValueError(f"LBank API error: {error_msg} (code: {error_code})")
            
            # Get exchange order ID from response
            exchange_order_id = str(response.get("data", {}).get("order_id", ""))
            
            # Start tracking order
            tracked_order = self._order_tracker.fetch_tracked_order(order_id)
            if tracked_order is None:
                self.logger().info(f"Starting to track order {order_id}")
                
                # Create InFlightOrder
                in_flight_order = InFlightOrder(
                    client_order_id=order_id,
                    exchange_order_id=exchange_order_id,
                    trading_pair=trading_pair,
                    order_type=order_type,
                    trade_type=trade_type,
                    price=price,
                    amount=amount,
                    creation_timestamp=self.current_timestamp
                )
                
                # Add to order tracker
                self._order_tracker.start_tracking_order(in_flight_order)
                
                self.logger().info(
                    f"Order tracking started:\n"
                    f"Client order ID: {order_id}\n"
                    f"Exchange order ID: {exchange_order_id}\n"
                    f"Creation timestamp: {in_flight_order.creation_timestamp}"
                )
            
            return response

        except Exception as e:
            self.logger().error(f"Error creating order {order_id}: {str(e)}", exc_info=True)
            raise

    async def _get_current_server_time(self) -> float:
        """
        Gets the current server time from LBank
        Returns a coroutine that will resolve to the server time
        """
        response = await self._api_get(
            path_url=CONSTANTS.GET_TIMESTAMP_PATH_URL,
            limit_id=CONSTANTS.GET_TIMESTAMP_PATH_URL
        )
        return float(response.get("data", 0))

    async def _place_order(
            self,
            order_id: str,
            trading_pair: str,
            amount: Decimal,
            trade_type: TradeType,
            order_type: OrderType,
            price: Decimal,
            **kwargs) -> Tuple[str, float]:
        """
        Place an order with logging and error handling
        """
        try:
            self.logger().info(
                f"Placing {trade_type.name} {order_type.name} order {order_id} "
                f"for {trading_pair} at {price} for {amount}"
            )
            
            creation_response = await self._create_order(
                trade_type=trade_type,
                order_id=order_id,
                trading_pair=trading_pair,
                amount=amount,
                order_type=order_type,
                price=price,
                **kwargs
            )
            
            self.logger().info(f"Full creation response: {creation_response}")
            
            # Extract order_id from the correct response field
            exchange_order_id = str(creation_response.get("data", {}).get("order_id", ""))
            if not exchange_order_id:
                self.logger().error(f"No order_id in response: {creation_response}")
                raise ValueError("No order_id received from exchange")
                
            timestamp = self._time_synchronizer.time()
            
            self.logger().info(f"Order {order_id} placed successfully with exchange ID: {exchange_order_id}")
            return exchange_order_id, timestamp
            
        except Exception as e:
            self.logger().error(f"Error placing order {order_id}: {str(e)}", exc_info=True)
            raise

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        """
        Cancels an order on LBank exchange.
        """
        try:
            self.logger().info(f"Canceling order {order_id} for {tracked_order.trading_pair}")
            
            exchange_trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
            
            # Create cancel order parameters
            params = {
                "symbol": exchange_trading_pair,
                "order_id": tracked_order.exchange_order_id
            }
            
            # Create REST request
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            request = RESTRequest(
                method=RESTMethod.POST,
                url=web_utils.private_rest_url(path_url=CONSTANTS.CANCEL_ORDER_PATH_URL),
                data=params,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                is_auth_required=True
            )

            self.logger().info(f"Sending cancel request for order {order_id}: {request}")
            raw_response = await rest_assistant.call(request)
            
            response = await raw_response.json()
            self.logger().info(f"Cancel order response for {order_id}: {response}")
            
            # Check if the cancellation was successful
            if response.get("result") is True:
                self.logger().info(f"Successfully canceled order {order_id}")
                return True
            else:
                error_msg = response.get("msg", "Unknown error")
                error_code = response.get("error_code", "Unknown code")
                self.logger().error(f"Failed to cancel order {order_id}: {error_msg} (code: {error_code})")
                return False

        except Exception as e:
            self.logger().error(f"Error canceling order {order_id}: {str(e)}", exc_info=True)
            return False

    async def _format_trading_rules(self, exchange_info: Dict[str, Any]) -> Dict[str, TradingRule]:
        """
        Converts json API response into a dictionary of trading rules.
        
        :param exchange_info: The json API response
        :return A dictionary of trading pairs mapping to their respective TradingRule
        """
        try:
            self.logger().info("Starting to format trading rules...")
            
            trading_rules = {}
            
            # Get the data array from exchange info
            pairs_info = exchange_info.get("data", [])
            if not isinstance(pairs_info, list):
                self.logger().error(f"Unexpected pairs_info type: {type(pairs_info)}")
                return trading_rules
            
            self.logger().info(f"Processing {len(pairs_info)} trading pairs")
            
            # Process each pair's trading rules
            for pair_info in pairs_info:
                try:
                    self.logger().debug(f"Processing pair info: {pair_info}")
                    
                    # Skip if not a valid dictionary
                    if not isinstance(pair_info, dict):
                        self.logger().warning(f"Skipping invalid pair info (not a dict): {pair_info}")
                        continue
                    
                    # Get required fields
                    symbol = pair_info.get("symbol")
                    price_accuracy = pair_info.get("priceAccuracy")
                    quantity_accuracy = pair_info.get("quantityAccuracy")
                    min_tran_qua = pair_info.get("minTranQua", "0")
                    
                    # Validate required fields
                    if not all([symbol, price_accuracy, quantity_accuracy]):
                        self.logger().warning(f"Skipping pair info missing required fields: {pair_info}")
                        continue
                    
                    # Convert trading pair using lbank_utils
                    trading_pair = lbank_utils.convert_from_exchange_trading_pair(symbol)
                    self.logger().info(f"Converting {symbol} to {trading_pair}")
                    
                    # Convert values to Decimal, with proper error handling
                    try:
                        min_quantity = Decimal(str(min_tran_qua))
                        price_precision = Decimal(str(price_accuracy))
                        quantity_precision = Decimal(str(quantity_accuracy))
                    except (TypeError, ValueError, decimal.InvalidOperation) as e:
                        self.logger().error(f"Error converting values for {trading_pair}: {str(e)}")
                        continue
                    
                    self.logger().info(
                        f"Trading rule parameters for {trading_pair}:\n"
                        f"  min_quantity: {min_quantity}\n"
                        f"  price_precision: {price_precision}\n"
                        f"  quantity_precision: {quantity_precision}"
                    )
                    
                    # Create TradingRule instance
                    trading_rule = TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=min_quantity,
                        min_price_increment=Decimal("1") / Decimal(str(10 ** int(price_precision))),
                        min_base_amount_increment=Decimal("1") / Decimal(str(10 ** int(quantity_precision)))
                    )
                    
                    trading_rules[trading_pair] = trading_rule
                    self.logger().info(f"Added trading rule for {trading_pair}: {trading_rule}")
                    
                except Exception as e:
                    self.logger().error(f"Error processing trading pair info: {str(e)}", exc_info=True)
                    continue
            
            self.logger().info(f"Completed formatting trading rules. Total rules: {len(trading_rules)}")
            self.logger().info(f"Trading pairs with rules: {list(trading_rules.keys())}")
            return trading_rules
            
        except Exception as e:
            self.logger().error(f"Error formatting trading rules: {str(e)}", exc_info=True)
            raise

    async def _update_trading_rules(self):
        """
        Updates the trading rules by fetching the latest exchange information
        """
        try:
            self.logger().info("Updating trading rules...")
            
            # Get all accuracy info in one request
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            accuracy_request = RESTRequest(
                method=RESTMethod.GET,
                url=web_utils.public_rest_url(path_url=CONSTANTS.EXCHANGE_INFO_PATH_URL),
                is_auth_required=False
            )
            
            accuracy_response = await rest_assistant.call(accuracy_request)
            response_data = await accuracy_response.json()
            
            # Extract trading rules data from response
            if isinstance(response_data, dict) and "data" in response_data:
                trading_rules_data = response_data["data"]
                if not isinstance(trading_rules_data, list):
                    self.logger().error(f"Unexpected trading rules data format: {trading_rules_data}")
                    return
            else:
                self.logger().error(f"Unexpected response format: {response_data}")
                return
            
            # Format the trading rules
            trading_rules = {}
            
            for rule_data in trading_rules_data:
                try:
                    if not isinstance(rule_data, dict):
                        continue
                    
                    # Get required fields
                    symbol = rule_data.get("symbol")
                    price_accuracy = rule_data.get("priceAccuracy")
                    quantity_accuracy = rule_data.get("quantityAccuracy")
                    min_tran_qua = rule_data.get("minTranQua", "0")
                    
                    # Validate required fields
                    if not all([symbol, price_accuracy is not None, quantity_accuracy is not None]):
                        self.logger().warning(f"Skipping rule data missing required fields: {rule_data}")
                        continue
                    
                    # Convert trading pair
                    trading_pair = lbank_utils.convert_from_exchange_trading_pair(symbol)
                    
                    # Create trading rule
                    trading_rule = TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=Decimal(str(min_tran_qua)),
                        min_price_increment=Decimal("1") / Decimal(str(10 ** int(price_accuracy))),
                        min_base_amount_increment=Decimal("1") / Decimal(str(10 ** int(quantity_accuracy)))
                    )
                    
                    trading_rules[trading_pair] = trading_rule
                    self.logger().debug(f"Created trading rule for {trading_pair}: {trading_rule}")
                    
                except Exception as e:
                    self.logger().error(f"Error creating trading rule from {rule_data}: {str(e)}")
                    continue
            
            # Update trading rules
            self._trading_rules.clear()
            for trading_pair, trading_rule in trading_rules.items():
                self._trading_rules[trading_pair] = trading_rule
            
            self.logger().info(f"Trading rules updated successfully. Total rules: {len(self._trading_rules)}")
            self.logger().debug(f"Trading pairs with rules: {list(self._trading_rules.keys())}")
            
        except Exception as e:
            self.logger().error(f"Error updating trading rules: {str(e)}", exc_info=True)
            raise

    async def _status_polling_loop_fetch_updates(self):
        await self._update_order_fills_from_trades()
        await super()._status_polling_loop_fetch_updates()

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _user_stream_event_listener(self):
        """Process user stream events from WebSocket"""
        try:
            async for event_message in self._iter_user_event_queue():
                try:
                    event_type = event_message.get("type")
                    
                    if event_type == "orderUpdate":
                        order_data = event_message.get("orderUpdate", {})
                        client_order_id = order_data.get("customerID")
                        exchange_order_id = order_data.get("uuid")
                        order_status_str = str(order_data.get("orderStatus", "0"))
                        
                        self.logger().info(
                            f"Order update received:\n"
                            f"Client ID: {client_order_id}\n"
                            f"Exchange ID: {exchange_order_id}\n"
                            f"Status: {order_status_str}\n"
                            f"Active orders before update: {len(self._order_tracker.active_orders)}\n"
                            f"Active order IDs: {list(self._order_tracker.active_orders.keys())}"
                        )
                        
                        if not client_order_id:
                            continue

                        # Immediately remove cancelled/filled orders
                        if order_status_str in ["-1", "2", "3", "4"]:  # Cancelled, Filled, Partially Filled Cancelled, Cancelling
                            if client_order_id in self._order_tracker.active_orders:
                                del self._order_tracker.active_orders[client_order_id]
                                self.logger().info(
                                    f"Removed order {client_order_id} due to final state {order_status_str}\n"
                                    f"Remaining active orders: {len(self._order_tracker.active_orders)}\n"
                                    f"Remaining order IDs: {list(self._order_tracker.active_orders.keys())}"
                                )
                            continue
                        
                        # Map LBank order status to OrderState
                        new_state = CONSTANTS.ORDER_STATE.get(order_status_str)
                        if new_state is None:
                            self.logger().warning(f"Unknown order status received: {order_status_str}")
                            continue

                        # Create order update
                        order_update = OrderUpdate(
                            client_order_id=client_order_id,
                            exchange_order_id=exchange_order_id,
                            trading_pair=order_data["symbol"].replace("_", "-").upper(),
                            update_timestamp=float(order_data.get("updateTime", time.time() * 1000)) * 1e-3,
                            new_state=new_state,
                        )
                        
                        # Process the update
                        self._order_tracker.process_order_update(order_update)
                        
                        # Log final state
                        self.logger().info(
                            f"Order state updated:\n"
                            f"Client ID: {client_order_id}\n"
                            f"New state: {new_state}\n"
                            f"Active orders after update: {len(self._order_tracker.active_orders)}\n"
                            f"Active order IDs: {list(self._order_tracker.active_orders.keys())}"
                        )

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger().error(f"Unexpected error in user stream listener: {str(e)}", exc_info=True)
                
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(f"Fatal error in user stream listener: {str(e)}", exc_info=True)

    async def _update_order_fills_from_trades(self):
        """
        Update order fills from order history.
        """
        try:
            for trading_pair in self._trading_pairs:
                exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
                
                # Create base parameters
                params = {
                    "symbol": exchange_symbol,
                    "current_page": "1",
                    "page_length": "100"
                }
                
                # Create REST request - let auth flow handle the signature
                rest_assistant = await self._web_assistants_factory.get_rest_assistant()
                request = RESTRequest(
                    method=RESTMethod.POST,
                    url=web_utils.private_rest_url(path_url=CONSTANTS.ORDER_INFO_HISTORY_PATH_URL),
                    data=params,  # Pass raw params, let auth handle it
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    is_auth_required=True
                )

                self.logger().info(f"Requesting order fills for {trading_pair}")
                raw_response = await rest_assistant.call(request)
                
                response = await raw_response.json()
                self.logger().debug(f"Order fills response for {trading_pair}: {response}")

                if response.get("result") is True and "data" in response:
                    orders_data = response["data"].get("orders", [])  # Get orders from the correct field
                    if not isinstance(orders_data, list):
                        self.logger().error(f"Unexpected orders array format: {orders_data}")
                        continue

                    for order in orders_data:
                        try:
                            # Validate order data
                            if not isinstance(order, dict):
                                self.logger().error(f"Invalid order data format: {order}")
                                continue

                            status = str(order.get("status", "0"))
                            if status not in ["1", "2", "3"]:  # Only process orders with valid status
                                continue

                            executed_amount = Decimal(str(order.get("deal_amount", "0")))
                            if executed_amount == Decimal("0"):
                                continue

                            # Calculate price and amounts
                            price = Decimal(str(order.get("price", "0")))
                            quote_amount = executed_amount * price

                            # Determine trade type
                            trade_type = TradeType.BUY if order.get("type", "").lower() == "buy" else TradeType.SELL

                            # Create trade update
                            trade_update = TradeUpdate(
                                trade_id=f"{order.get('order_id', '')}_{order.get('create_time', '')}",
                                client_order_id=order.get("custom_id", ""),
                                exchange_order_id=str(order.get("order_id", "")),
                                trading_pair=trading_pair,
                                fee=TradeFeeBase.new_spot_fee(
                                    fee_schema=self.trade_fee_schema(),
                                    trade_type=trade_type,
                                ),
                                fill_base_amount=executed_amount,
                                fill_quote_amount=quote_amount,
                                fill_price=price,
                                fill_timestamp=float(order.get("create_time", self._time())) * 1e-3,
                            )
                            
                            self._order_tracker.process_trade_update(trade_update)
                            self.logger().info(f"Processed trade update for order {order.get('order_id', '')}")
                            
                        except Exception as e:
                            self.logger().error(f"Error processing order {order}: {str(e)}")
                            continue

                else:
                    error_msg = response.get("msg", "Unknown error")
                    error_code = response.get("error_code", "Unknown code")
                    self.logger().error(f"Error in order history response: {error_msg} (code: {error_code})")
            
            self._last_trades_poll_lbank_timestamp = self._time()
                
        except Exception as e:
            self.logger().error(f"Error fetching order history update: {str(e)}", exc_info=True)
            raise

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            exchange_order_id = int(order.exchange_order_id)
            trading_pair = lbank_utils.convert_to_exchange_trading_pair(order.trading_pair)
            
            # Prepare parameters according to LBank API requirements
            data = {
                "orderId": str(exchange_order_id),
                "symbol": trading_pair,
            }
            
            # Add auth parameters and get headers
            auth_headers = self._auth.header_for_authentication()
            data = self._auth.add_auth_to_params(data)
            
            # Ensure Content-Type header is set
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                **auth_headers
            }
            
            # URL encode the data
            encoded_data = urlencode(data)
            
            all_fills_response = await self._api_post(
                path_url=CONSTANTS.MY_TRADES_PATH_URL,
                data=encoded_data,  # Send URL-encoded data
                headers=headers,
                is_auth_required=True,
                limit_id=CONSTANTS.MY_TRADES_PATH_URL)

            if isinstance(all_fills_response, dict) and all_fills_response.get("result") == "true":
                trades = all_fills_response.get("data", [])
                for trade in trades:
                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=trade.get("type", ""),
                        percent_token=trade.get("fee_coin", ""),
                        flat_fees=[TokenAmount(
                            amount=Decimal(str(trade.get("fee", "0"))),
                            token=trade.get("fee_coin", "")
                        )]
                    )
                    trade_update = TradeUpdate(
                        trade_id=str(trade.get("trade_id", "")),
                        client_order_id=order.client_order_id,
                        exchange_order_id=str(trade.get("order_id", "")),
                        trading_pair=trading_pair,
                        fee=fee,
                        fill_base_amount=Decimal(str(trade.get("amount", "0"))),
                        fill_quote_amount=Decimal(str(trade.get("total", "0"))),
                        fill_price=Decimal(str(trade.get("price", "0"))),
                        fill_timestamp=float(trade.get("time", 0)) * 1e-3,
                    )
                    trade_updates.append(trade_update)

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Request order status from the exchange
        """
        try:
        self.logger().error("LOGGER TEST - START OF ORDER STATUS")
            
            exchange_trading_pair = await self.exchange_symbol_associated_to_pair(tracked_order.trading_pair)
            
            params = {
                "symbol": exchange_trading_pair,
                "order_id": tracked_order.exchange_order_id,
            }
            
            self.logger().error(f"[ORDER STATUS] Requesting with data: {params}")

            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            request = RESTRequest(
                method=RESTMethod.POST,
                url=web_utils.private_rest_url(path_url=CONSTANTS.ORDER_INFO_URL),
                data=params,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                is_auth_required=True
            )
            
            response = await rest_assistant.call(request)
            data = await response.json()
            
            if not data.get("result", False):
                raise ValueError(f"Error getting order status: {data.get('error_code')} - {data.get('msg')}")
            
            order_data = data.get("data", [{}])[0]  # LBank returns array with single order
            
            # Map order status
            status = str(order_data.get("status", "0"))
            new_state = CONSTANTS.ORDER_STATE.get(status, OrderState.FAILED)
            
            # Create order update
            order_update = OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=new_state
            )

            return order_update
            
        except Exception as e:
            self.logger().error(
                f"[ORDER STATUS] Error getting order status:\n"
                f"Trading pair: {tracked_order.trading_pair}\n"
                f"Order ID: {tracked_order.exchange_order_id}\n"
                f"Client Order ID: {tracked_order.client_order_id}\n"
                f"Error: {str(e)}",
                exc_info=True
            )
            raise

    async def _update_balances(self):
        """Update user balances."""
        try:
            # Get account info with balances
            account_info = await self._api_post(
                path_url=self._user_info_url,
                data={},  # Empty dict for required POST request
                is_auth_required=True
            )
            
            self.logger().debug(f"Account info response: {account_info}")
            
            # Clear existing balances
            self._account_available_balances.clear()
            self._account_balances.clear()
            
            # Handle LBank's updated balance format
            if isinstance(account_info, dict) and account_info.get("result") == "true" and "data" in account_info:
                for balance_entry in account_info["data"]:
                    asset = balance_entry.get("coin", "").upper()  # Get asset and convert to upper case
                    
                    # Convert string amounts to Decimal
                    available_amount = Decimal(str(balance_entry.get("usableAmt", "0")))
                    frozen_amount = Decimal(str(balance_entry.get("freezeAmt", "0")))
                    total_amount = Decimal(str(balance_entry.get("assetAmt", "0")))
                    
                    # Update the balance dictionaries
                    self._account_balances[asset] = total_amount
                    self._account_available_balances[asset] = available_amount
                    
                self.logger().info(f"Balances updated successfully for {len(self._account_balances)} assets")
            else:
                self.logger().error(f"Unexpected balance response format: {account_info}")
                raise ValueError("Failed to parse balance response")
            
        except Exception as e:
            self.logger().error(f"Could not update balances: {str(e)}", exc_info=True)
            raise

    def _generate_echostr(self, length: int = 35) -> str:
        """Generate a random echostr of specified length"""
        import random
        import string
        characters = string.ascii_letters + string.digits
        return ''.join(random.choice(characters) for _ in range(length))

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """
        Fixed trading pair initialization
        """
        try:
            # Make sure we're accessing the correct data structure
            trading_pairs_data = exchange_info.get("data", [])
            if not trading_pairs_data:
                self.logger().error("No trading pairs data found in exchange info")
                return

            for symbol_data in trading_pairs_data:
                if not lbank_utils.is_exchange_information_valid(symbol_data):
                    continue
                    
                # LBank uses underscore separator, we need to convert to dash
                exchange_symbol = symbol_data["symbol"]  # e.g. "brg_usdt"
                trading_pair = exchange_symbol.replace("_", "-").upper()  # e.g. "BRG-USDT"
                
                # Store both mappings for bidirectional conversion
                self._trading_pair_symbol_map[trading_pair] = exchange_symbol
                self._symbol_trading_pair_map[exchange_symbol] = trading_pair
                
                self.logger().info(f"Initialized trading pair mapping: {trading_pair} -> {exchange_symbol}")

            # Initialize order tracking with ClientOrderTracker
            self._order_tracker = ClientOrderTracker(connector=self)
            self.logger().info("Initialized client order tracker")

        except Exception as e:
            self.logger().error(f"Error initializing trading pair symbols: {str(e)}", exc_info=True)
            raise

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        params = {
            "symbol": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        }

        resp_json = await self._api_get(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL,
            params=params
        )

        return float(resp_json["lastPrice"])

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        """
        Satisfies the abstract method from ExchangePyBase and
        creates a data source (object) to retrieve order book data.

        Update "LbankAPIOrderBookDataSource" import/path accordingly
        if the class is in a different module.
        """
        return LbankAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain  # use or remove domain as needed
        )

    async def get_token_price_in_usdt(self, token: str) -> Optional[Decimal]:
        """
        Gets the USDT price for a specific token using LBank's price endpoint
        :param token: The token symbol (e.g., 'BTC', 'ETH')
        :return: Price in USDT if available, None otherwise
        """
        try:
            if token == "USDT":
                return Decimal("1.0")
            
            symbol = f"{token.lower()}_usdt"
            self.logger().info(f"Fetching price for {symbol}")
            
            response = await self._api_get(
                path_url=CONSTANTS.TICKER_PRICE_PATH_URL,
                params={"symbol": symbol}
            )
            
            self.logger().info(f"Raw price response for {symbol}: {response}")
            
            # Handle the actual response format from LBank
            if isinstance(response, dict):
                # Check if it's a successful response
                if response.get("result") is True and "data" in response:
                    price_data = response["data"][0]
                    if isinstance(price_data, dict) and "price" in price_data:
                        try:
                            price = Decimal(str(price_data["price"]))
                            self.logger().info(f"✓ Got price for {token}: {price} USDT")
                            return price
                        except (TypeError, ValueError) as e:
                            self.logger().error(f"Error converting price value for {token}: {e}")
                # Handle error response
                elif response.get("result") == "false" and "error_code" in response:
                    self.logger().info(f"Price not available for {token}: {response.get('msg')}")
                else:
                    self.logger().error(f"Unexpected response format: {response}")
            
            return None
        except Exception as e:
            self.logger().error(f"Error fetching price for {token}: {str(e)}")
            return None

    async def all_balances(self) -> str:
        """
        Ensures balances are updated before displaying them
        """
        await self._update_balances()
        return self.get_balance_display()

    async def exchange_symbol_associated_to_pair(self, trading_pair: str) -> str:
        """Convert a trading pair to exchange symbol format."""
        return lbank_utils.convert_to_exchange_trading_pair(trading_pair)

    async def trading_pair_associated_to_exchange_symbol(self, symbol: str) -> str:
        """Convert an exchange symbol to Hummingbot trading pair format."""
        return lbank_utils.convert_from_exchange_trading_pair(symbol)

    async def _api_post(
        self,
        path_url: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = False,
        limit_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Sends a POST request to LBank API
        """
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        
        if data is None:
            data = {}
            
        # Set required headers
        if headers is None:
            headers = {}
        headers.update({
            "Content-Type": "application/x-www-form-urlencoded",
        })
        
        try:
            # Get raw response first, similar to user stream data source
            raw_response = await rest_assistant.execute_request_and_get_response(
                url=self.web_utils.private_rest_url(path_url=path_url),
                data=data,
                params=params,
                headers=headers, 
                method=RESTMethod.POST,
                is_auth_required=is_auth_required,
                throttler_limit_id=limit_id if limit_id else path_url,
            )

            # Get the raw text first, like in LbankAPIUserStreamDataSource
            raw_text = await raw_response.text()
            self.logger().debug(f"Raw response text from {path_url}: {raw_text}")
            
            # Try to parse it as JSON
            try:
                parsed_response = json.loads(raw_text)
            except json.JSONDecodeError:
                self.logger().error(f"Non-JSON response from {path_url}: {raw_text}")
                raise ValueError(f"Invalid JSON response from {path_url}: {raw_text}")
            
            # Handle error responses according to docs
            if isinstance(parsed_response, dict):
                if not parsed_response.get("result", True):
                    error_msg = parsed_response.get("msg", "Unknown error")
                    error_code = parsed_response.get("error_code", "Unknown code") 
                    raise ValueError(f"LBank API error: {error_msg} (code: {error_code})")
                    
            return parsed_response
            
        except Exception as e:
            self.logger().error(f"Error making POST request to {path_url}: {str(e)}")
            self.logger().error(f"Request data: {data}")
            raise

    def _initialize_markets(self, market_names: List[Tuple[str, List[str]]]):
        """
        Initialize markets and ensure balances are properly set
        """
        self._trading_required = False
        
        # Initialize trading pairs
        for market_name, trading_pairs in market_names:
            if market_name not in self.market_trading_pairs_map:
                self.market_trading_pairs_map[market_name] = []
            for hb_trading_pair in trading_pairs:
                self.market_trading_pairs_map[market_name].append(hb_trading_pair)
                
                # Initialize balances for the trading pair tokens
                base, quote = self.split_trading_pair(hb_trading_pair)
                for token in [base, quote]:
                    if token not in self._account_balances:
                        self._account_balances[token] = s_decimal_0
                    if token not in self._account_available_balances:
                        self._account_available_balances[token] = s_decimal_0
        
        super()._initialize_markets(market_names)
        self._trading_rules = {}
        self._trading_required = True

    async def _update_order_status(self):
        """
        Checks and updates the status of active orders
        """
        if not self._trading_required:
            return
        
        active_orders = list(self._order_tracker.active_orders.values())
        if not active_orders:
            return
        
        for order in active_orders:
            try:
                order_age = self.current_timestamp - order.creation_timestamp
                
            self.logger().info(
                f"Checking order {order.client_order_id}:\n"
                    f"Age: {order_age} seconds\n"
                f"Creation time: {order.creation_timestamp}\n"
                    f"Current time: {self.current_timestamp}\n"
                    f"Refresh time: {self._order_refresh_time}\n"
                f"Current state: {order.current_state}"
            )
            
                # Cancel orders that have exceeded refresh time
                if order_age >= self._order_refresh_time:
                self.logger().info(
                        f"Order {order.client_order_id} has exceeded refresh time "
                        f"({order_age} >= {self._order_refresh_time}). Cancelling..."
                    )
                    await self.cancel_order(order.client_order_id)
                    continue
                
                # Update order status
                order_update = await self._request_order_status(order)
                self._order_tracker.process_order_update(order_update)
                
            except Exception as e:
                self.logger().error(
                    f"Error updating order {order.client_order_id} status: {str(e)}",
                    exc_info=True
                )

    @property
    def ready(self) -> bool:
        """
        Checks if the connector is ready for trading
        """
        try:
            # Check if trading rules are initialized
            if not self._trading_rules:
                self.logger().info("Not ready - Trading rules not initialized")
                return False
                
            for trading_pair in self._trading_pairs:
                if trading_pair not in self._trading_rules:
                    self.logger().info(f"Not ready - Trading rules not initialized for {trading_pair}")
                    return False
                    
                # Check if order book exists
                if trading_pair not in self.order_books:
                    self.logger().info(f"Not ready - Order book not initialized for {trading_pair}")
                    return False
                
            # Check order tracker is initialized
            if self._order_tracker is None:
                self.logger().info("Not ready - Order tracker not initialized")
                return False
            
            # Log current state
            self.logger().info(
                f"Checking readiness:\n"
                f"Active orders: {len(self._order_tracker.active_orders)}\n"
                f"Trading rules initialized: {bool(self._trading_rules)}\n"
                f"Order books initialized: {all(tp in self.order_books for tp in self._trading_pairs)}"
            )
            
            return True
            
        except Exception as e:
            self.logger().error(f"Error checking readiness: {str(e)}", exc_info=True)
            return False

    def get_price_by_type(self, trading_pair: str, price_type: PriceType) -> Decimal:
        order_book = self.get_order_book(trading_pair)
        if order_book is None:
            raise ValueError(f"Order book for {trading_pair} is not available.")
        
        # Convert both prices to Decimal before calculation
        best_bid = Decimal(str(order_book.get_price(True)))
        best_ask = Decimal(str(order_book.get_price(False)))
        mid_price = (best_bid + best_ask) / Decimal("2")
        
        # Return different price types based on the request
        if price_type == PriceType.MidPrice:
            return mid_price
        elif price_type == PriceType.BestBid:
            return best_bid
        elif price_type == PriceType.BestAsk:
            return best_ask
        else:
            raise ValueError(f"Unrecognized price type: {price_type}")

    def get_order_book(self, trading_pair: str) -> OrderBook:
        self.logger().info(f"[ORDER BOOK] Getting order book for {trading_pair}")
        self.logger().info(f"[ORDER BOOK] Available order books: {list(self.order_books.keys())}")
        
        if trading_pair not in self.order_books:
            self.logger().error(f"[ORDER BOOK] No order book exists for '{trading_pair}'")
            self.logger().info(f"[ORDER BOOK] Current order tracker state: {self._order_tracker.active_orders}")
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        
        order_book = self.order_books[trading_pair]
        if order_book is None:
            self.logger().error(f"[ORDER BOOK] Order book is None for {trading_pair}")
            raise ValueError(f"Order book is None for {trading_pair}")
        
        self.logger().info(f"[ORDER BOOK] Retrieved order book for {trading_pair}: {order_book}")
        return order_book

    async def _initialize_trading_pair_symbol_map(self):
        """
        Initialize the trading pair symbol map. This method is called by the base class's trading_pair_symbol_map() method.
        """
        try:
            # Get exchange info
            exchange_info = await self._api_get(
                path_url=self.trading_pairs_request_path,
            )
            
            # Initialize the bidict for trading pair mapping
            mapping_dict = bidict()
            
            # Process the exchange info
            trading_pairs_data = exchange_info.get("data", [])
            if not trading_pairs_data:
                self.logger().error("No trading pairs data found in exchange info")
                return
            
            for symbol_data in trading_pairs_data:
                if not lbank_utils.is_exchange_information_valid(symbol_data):
                    continue
                    
                # LBank uses underscore separator, we need to convert to dash
                exchange_symbol = symbol_data["symbol"]  # e.g. "brg_usdt"
                trading_pair = exchange_symbol.replace("_", "-").upper()  # e.g. "BRG-USDT"
                
                # Store in bidict
                mapping_dict[exchange_symbol] = trading_pair
                
                self.logger().info(f"Initialized trading pair mapping: {exchange_symbol} -> {trading_pair}")
            
            self._set_trading_pair_symbol_map(mapping_dict)

        except Exception as e:
            self.logger().error(f"Error initializing trading pair symbol map: {str(e)}", exc_info=True)
            raise

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        """
        Cancels all active orders by using the cancel_order_by_symbol endpoint for each trading pair.
        """
        cancellation_results = []
        try:
            self.logger().info(
                f"Starting cancel_all:\n"
                f"Current active orders: {len(self._order_tracker.active_orders)}\n"
                f"Active order IDs: {list(self._order_tracker.active_orders.keys())}"
            )
            
            tasks = []
            for trading_pair in self._trading_pairs:
                exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
                
                # Create REST request for cancelling all orders for this symbol
                rest_assistant = await self._web_assistants_factory.get_rest_assistant()
                request = RESTRequest(
                    method=RESTMethod.POST,
                    url=web_utils.private_rest_url(path_url=CONSTANTS.CANCEL_ORDERS_BY_PAIR_PATH_URL),
                    data={"symbol": exchange_symbol},
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    is_auth_required=True
                )
                tasks.append((trading_pair, rest_assistant.call(request)))

            async with timeout(timeout_seconds):
                for trading_pair, request_task in tasks:
                    try:
                        response = await request_task
                        response_json = await response.json()
                        
                        # Force cleanup of active orders for this trading pair
                        active_orders = list(self._order_tracker.active_orders.values())
                        pair_orders = [o for o in active_orders if o.trading_pair == trading_pair]
                        
                        for order in pair_orders:
                            if order.client_order_id in self._order_tracker.active_orders:
                                del self._order_tracker.active_orders[order.client_order_id]
                                self.logger().info(
                                    f"Removed order {order.client_order_id} during cancel_all\n"
                                    f"Remaining active orders: {len(self._order_tracker.active_orders)}"
                                )
                        
                        cancellation_results.append(CancellationResult(trading_pair, True))

                    except Exception as e:
                        self.logger().error(f"Error cancelling orders for {trading_pair}: {str(e)}")
                        cancellation_results.append(CancellationResult(trading_pair, False))

            # Final cleanup - remove any remaining active orders
            self._order_tracker.active_orders.clear()
            
        self.logger().info(
                f"Cancel all completed:\n"
                f"Successful cancellations: {len([r for r in cancellation_results if r.success])}\n"
                f"Failed cancellations: {len([r for r in cancellation_results if not r.success])}\n"
                f"Remaining active orders: {len(self._order_tracker.active_orders)}"
            )
            
        return cancellation_results

        except Exception as e:
            self.logger().error(f"Error in cancel_all: {str(e)}", exc_info=True)
            return [CancellationResult(trading_pair, False) for trading_pair in self._trading_pairs]

    async def _execute_cancel(self, trading_pair: str, order_id: str) -> str:
        """
        Executes order cancellation
        """
        try:
            self.logger().info(
                f"Starting cancellation for order {order_id} on {trading_pair}\n"
                f"Current time: {time.time()}\n"
                f"Active orders: {list(self._order_tracker.active_orders.keys())}"
            )
            
            exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
            tracked_order = self._order_tracker.active_orders.get(order_id)

            if tracked_order:
                self.logger().info(
                    f"Order details before cancellation:\n"
                    f"Creation time: {tracked_order.creation_timestamp}\n"
                    f"Last update: {tracked_order.last_update_timestamp}\n"
                    f"Time since creation: {time.time() - tracked_order.creation_timestamp} seconds\n"
                    f"Current state: {tracked_order.current_state}\n"
                    f"Is done: {tracked_order.is_done}\n"
                    f"Is cancelled: {tracked_order.is_cancelled}\n"
                    f"Is filled: {tracked_order.is_filled}"
                )

            # First find the exchange_order_id using orders_info_no_deal endpoint
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            active_orders_request = RESTRequest(
                method=RESTMethod.POST,
                url=web_utils.private_rest_url(path_url="/v2/supplement/orders_info_no_deal.do"),
                data={
                    "symbol": exchange_symbol,
                    "current_page": "1",
                    "page_length": "200"
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                is_auth_required=True
            )
            
            try:
                self.logger().debug(f"Querying active orders for {exchange_symbol}")
                response = await rest_assistant.call(active_orders_request)
                response_data = await response.json()
                
                self.logger().debug(f"Active orders response: {response_data}")
                
                exchange_order_id = None
                if response_data.get("result") is True and "data" in response_data:
                    orders = response_data["data"]["orders"]
                    self.logger().info(
                        f"Found {len(orders)} active orders for {exchange_symbol}\n"
                        f"Looking for order {order_id}"
                    )
                    
                    # Find orders that are not cancelled or filled (status 0 or 1)
                    matching_order = next(
                        (order for order in orders 
                         if order.get("clientOrderId") == order_id 
                         and str(order.get("status")) in ["0", "1"]),
                        None
                    )
                    
                    if matching_order:
                        exchange_order_id = matching_order.get("orderId")
                        self.logger().info(
                            f"Found active order to cancel:\n"
                            f"Client ID: {order_id}\n"
                            f"Exchange ID: {exchange_order_id}\n"
                            f"Status: {matching_order.get('status')}"
                        )
                    else:
                        self.logger().warning(
                            f"No active order found matching {order_id}"
                        )
                        # Remove from active orders since order is not active
                        if order_id in self._order_tracker.active_orders:
                            del self._order_tracker.active_orders[order_id]
                            self.logger().info(f"Removed {order_id} from active_orders")
                        return None
                else:
                    self.logger().error(f"Failed to get active orders: {response_data.get('msg', 'Unknown error')}")
                    return None

                # Now send the actual cancellation to cancel_order endpoint
                if exchange_order_id:
                    cancel_request = RESTRequest(
                        method=RESTMethod.POST,
                        url=web_utils.private_rest_url(path_url="/v2/cancel_order.do"),
                        data={
                            "symbol": exchange_symbol,
                            "order_id": exchange_order_id
                        },
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        is_auth_required=True
                    )
                    
                    self.logger().info(
                        f"Sending cancel request:\n"
                        f"Client ID: {order_id}\n"
                        f"Exchange ID: {exchange_order_id}\n"
                        f"Current time: {time.time()}"
                    )
                    
                    cancel_response = await rest_assistant.call(cancel_request)
                    cancel_data = await cancel_response.json()
                    
                    self.logger().debug(f"Cancel response: {cancel_data}")
                    
                    if cancel_data.get("result") is True:
                        self.logger().info(
                            f"Successfully cancelled order:\n"
                            f"Client ID: {order_id}\n"
                            f"Exchange ID: {exchange_order_id}\n"
                            f"Time of cancellation: {time.time()}"
                        )
                        
                        # Update order tracker
                        if tracked_order is not None:
                            self._order_tracker.process_order_update(OrderUpdate(
                                client_order_id=order_id,
                                exchange_order_id=exchange_order_id,
                                trading_pair=trading_pair,
                                update_timestamp=self._time(),
                                new_state=OrderState.CANCELED
                            ))
                            
                        return order_id
                    else:
                        error_msg = cancel_data.get("error_message", cancel_data.get("msg", "Unknown error"))
                        self.logger().error(
                            f"Failed to cancel order {order_id}:\n"
                            f"Error: {error_msg}\n"
                            f"Time of failure: {time.time()}"
                        )
                        # Remove from active orders if the error indicates order doesn't exist
                        if "order not exist" in str(error_msg).lower():
                            if order_id in self._order_tracker.active_orders:
                                del self._order_tracker.active_orders[order_id]
                                self.logger().info(
                                    f"Removed non-existent order {order_id} from active_orders"
                                )
                        return None
            
            except Exception as e:
                self.logger().error(
                    f"Error in order cancellation process:\n"
                    f"Order ID: {order_id}\n"
                    f"Error: {str(e)}\n"
                    f"Time of error: {time.time()}",
                    exc_info=True
                )
                return None
            
        except Exception as e:
            self.logger().error(
                f"Failed to cancel order {order_id}:\n"
                f"Error: {str(e)}\n"
                f"Time of error: {time.time()}",
                exc_info=True
            )
            self.logger().network(
                "Failed to cancel order.",
                exc_info=True,
                app_warning_msg=f"Failed to cancel order {order_id}. Check API key and network connection."
            )
        return None

    def _time(self) -> float:
        """
        Returns current timestamp in float
        """
        return time.time()

    def should_create_new_orders(self) -> bool:
        """
        Determines whether new orders should be created based on the current state.
        """
        try:
            current_timestamp = self.current_timestamp
            active_orders = self._order_tracker.active_orders
            
            self.logger().info(
                f"Checking if should create new orders:\n"
                f"Active orders count: {len(active_orders)}\n"
                f"Current timestamp: {current_timestamp}"
            )
            
            # Clean up any orders that should no longer be active
            for order_id, order in list(active_orders.items()):
                order_age = current_timestamp - order.creation_timestamp
                
                self.logger().info(
                    f"Checking order {order_id}:\n"
                    f"Age: {order_age} seconds\n"
                    f"Creation timestamp: {order.creation_timestamp}\n"
                    f"Current timestamp: {current_timestamp}\n"
                    f"Refresh time: {self._order_refresh_time}"
                )
                
                # Remove orders that are done, cancelled, filled, or exceeded refresh time
                should_remove = (
                    order.is_done or 
                    order.is_cancelled or 
                    order.is_filled or 
                    order_age >= self._order_refresh_time or
                    order.current_state in {OrderState.CANCELED, OrderState.FILLED, OrderState.FAILED}
                )
                
                if should_remove:
                    self.logger().info(
                        f"Removing order {order_id} from active orders:\n"
                        f"Is done: {order.is_done}\n"
                        f"Is cancelled: {order.is_cancelled}\n"
                        f"Is filled: {order.is_filled}\n"
                        f"Age: {order_age} >= {self._order_refresh_time}"
                    )
                    del active_orders[order_id]
            
            # After cleanup, check if we should create new orders
            should_create = len(active_orders) == 0
            
            self.logger().info(
                f"Final decision - Should create new orders: {should_create}\n"
                f"Remaining active orders: {len(active_orders)}"
            )
            
            return should_create
            
        except Exception as e:
            self.logger().error(f"Error checking if should create new orders: {str(e)}", exc_info=True)
            return False
