from decimal import Decimal
import asyncio
from typing import Optional

from hummingbot.core.data_type.common import PriceType
from hummingbot.strategy.asset_price_delegate import AssetPriceDelegate
from hummingbot.connector.exchange_base import ExchangeBase
from pancakeswap_price_fetcher import PancakeSwapPriceFetcher

class NEIPancakeswapPriceDelegate(AssetPriceDelegate):
    def __init__(self, market: ExchangeBase, update_interval: float = 5.0):
        super().__init__()
        self._market = market
        self._update_interval = update_interval
        self._price_fetcher: Optional[PancakeSwapPriceFetcher] = None
        self._current_price = Decimal("0")
        self._fetch_price_task: Optional[asyncio.Task] = None
        self._ready = False
        self.start()

    def start(self):
        self._fetch_price_task = safe_ensure_future(self._price_update_loop())

    async def stop(self):
        if self._fetch_price_task is not None:
            self._fetch_price_task.cancel()
            self._fetch_price_task = None
        if self._price_fetcher is not None:
            await self._price_fetcher.__aexit__(None, None, None)
            self._price_fetcher = None

    async def _price_update_loop(self):
        while True:
            try:
                if self._price_fetcher is None:
                    self._price_fetcher = PancakeSwapPriceFetcher()
                    await self._price_fetcher.__aenter__()

                # Get NEI-WBNB prices
                nei_wbnb_price, _ = await self._price_fetcher.get_token_price(
                    "0x96e48ce48dce021796cb3cd8864226a8e64e1f7d",  # NEI-WBNB pool
                    18,  # NEI decimals
                    18   # WBNB decimals
                )
                
                # Get WBNB-BUSD prices
                wbnb_busd_price, _ = await self._price_fetcher.get_token_price(
                    "0x58F876857a02D6762E0101bb5C46A8c1ED44Dc16",  # WBNB-BUSD pool
                    18,  # WBNB decimals
                    18   # BUSD decimals
                )
                
                # Calculate and update NEI price in BUSD
                self._current_price = Decimal(str(nei_wbnb_price * wbnb_busd_price))
                self._ready = True

            except Exception as e:
                self._ready = False
                self.logger().network(
                    f"Error fetching NEI price from PancakeSwap: {str(e)}",
                    exc_info=True,
                    app_warning_msg="Could not fetch NEI price from PancakeSwap. Check network connection."
                )

            await asyncio.sleep(self._update_interval)

    def get_price_by_type(self, _: PriceType) -> Decimal:
        return self._current_price

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def market(self) -> ExchangeBase:
        return self._market 