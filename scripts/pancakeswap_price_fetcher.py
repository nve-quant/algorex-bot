#!/usr/bin/env python3

import asyncio
import logging
from decimal import Decimal
from typing import Optional, Tuple

from web3 import Web3
from web3.contract import Contract
from web3.exceptions import ContractLogicError
from eth_typing import Address
from aiohttp import ClientSession, ClientTimeout
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
BSC_RPC_ENDPOINTS = [
    "https://bsc-dataseed.binance.org/",
    "https://bsc-dataseed1.defibit.io/",
    "https://bsc-dataseed1.ninicoin.io/",
]
PANCAKESWAP_V2_FACTORY = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
PANCAKESWAP_V2_ROUTER = "0x10ED43C718714eb63d5aA57B78B54704E256024E"

# PancakeSwap V2 Pool ABI (minimal required functions)
POOL_ABI = json.dumps([
    {
        "inputs": [],
        "name": "getReserves",
        "outputs": [
            {"internalType": "uint112", "name": "_reserve0", "type": "uint112"},
            {"internalType": "uint112", "name": "_reserve1", "type": "uint112"},
            {"internalType": "uint32", "name": "_blockTimestampLast", "type": "uint32"}
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function"
    }
])

# ERC20 Token ABI (minimal required functions)
TOKEN_ABI = json.dumps([
    {
        "inputs": [],
        "name": "symbol",
        "outputs": [{"internalType": "string", "name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function"
    }
])

class PancakeSwapPriceFetcher:
    def __init__(self, rpc_endpoints: list = BSC_RPC_ENDPOINTS):
        self.rpc_endpoints = rpc_endpoints
        self.current_endpoint_index = 0
        self.web3 = Web3(Web3.HTTPProvider(self.rpc_endpoints[0]))
        self.session: Optional[ClientSession] = None
        
    async def __aenter__(self):
        timeout = ClientTimeout(total=10)
        self.session = ClientSession(timeout=timeout)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            
    def _get_pool_contract(self, pool_address: str) -> Contract:
        """Create a contract instance for the pool."""
        return self.web3.eth.contract(
            address=Web3.to_checksum_address(pool_address),
            abi=json.loads(POOL_ABI)
        )

    def _get_token_contract(self, token_address: str) -> Contract:
        """Create a contract instance for an ERC20 token."""
        return self.web3.eth.contract(
            address=Web3.to_checksum_address(token_address),
            abi=json.loads(TOKEN_ABI)
        )
        
    async def _try_alternate_rpc(self):
        """Switch to the next RPC endpoint if the current one fails."""
        self.current_endpoint_index = (self.current_endpoint_index + 1) % len(self.rpc_endpoints)
        self.web3 = Web3(Web3.HTTPProvider(self.rpc_endpoints[self.current_endpoint_index]))
        logger.info(f"Switched to RPC endpoint: {self.rpc_endpoints[self.current_endpoint_index]}")

    async def get_pool_info(self, pool_address: str) -> Tuple[str, str, int, int]:
        """Get token symbols and decimals from the pool."""
        pool_contract = self._get_pool_contract(pool_address)
        
        # Get token addresses
        token0_address = await asyncio.to_thread(pool_contract.functions.token0().call)
        token1_address = await asyncio.to_thread(pool_contract.functions.token1().call)
        
        # Get token contracts
        token0_contract = self._get_token_contract(token0_address)
        token1_contract = self._get_token_contract(token1_address)
        
        # Get symbols and decimals
        token0_symbol = await asyncio.to_thread(token0_contract.functions.symbol().call)
        token1_symbol = await asyncio.to_thread(token1_contract.functions.symbol().call)
        token0_decimals = await asyncio.to_thread(token0_contract.functions.decimals().call)
        token1_decimals = await asyncio.to_thread(token1_contract.functions.decimals().call)
        
        return token0_symbol, token1_symbol, token0_decimals, token1_decimals
        
    async def get_token_price(
        self,
        pool_address: str,
        decimals_token0: int,
        decimals_token1: int,
        retries: int = 3
    ) -> Tuple[Decimal, Decimal]:
        """
        Fetch the price of tokens in a PancakeSwap pool.
        
        Args:
            pool_address: The address of the PancakeSwap pool
            decimals_token0: Decimals of token0
            decimals_token1: Decimals of token1
            retries: Number of retry attempts
            
        Returns:
            Tuple[Decimal, Decimal]: (price of token0 in terms of token1, price of token1 in terms of token0)
        """
        for attempt in range(retries):
            try:
                pool_contract = self._get_pool_contract(pool_address)
                reserves = await asyncio.to_thread(
                    pool_contract.functions.getReserves().call
                )
                
                reserve0, reserve1, _ = reserves
                
                # Calculate prices with proper decimal handling
                price0_in_1 = Decimal(reserve1) * Decimal(10 ** (decimals_token0 - decimals_token1)) / Decimal(reserve0)
                price1_in_0 = Decimal(reserve0) * Decimal(10 ** (decimals_token1 - decimals_token0)) / Decimal(reserve1)
                
                return price0_in_1, price1_in_0
                
            except (ContractLogicError, Exception) as e:
                if attempt < retries - 1:
                    logger.warning(f"Error fetching price, attempt {attempt + 1}/{retries}: {str(e)}")
                    await self._try_alternate_rpc()
                    continue
                else:
                    logger.error(f"Failed to fetch price after {retries} attempts: {str(e)}")
                    raise

async def main():
    # Pool addresses
    NEI_POOL_ADDRESS = "0x96e48ce48dce021796cb3cd8864226a8e64e1f7d"  # NEI-WBNB pool
    WBNB_BUSD_POOL_ADDRESS = "0x58F876857a02D6762E0101bb5C46A8c1ED44Dc16"  # WBNB-BUSD pool
    
    from aiohttp import web
    import json

    async def get_price_handler(request):
        async with PancakeSwapPriceFetcher() as price_fetcher:
            try:
                # First get NEI-WBNB pool information
                token0_symbol, token1_symbol, token0_decimals, token1_decimals = await price_fetcher.get_pool_info(NEI_POOL_ADDRESS)
                
                # Get NEI-WBNB prices
                nei_wbnb_price, _ = await price_fetcher.get_token_price(
                    NEI_POOL_ADDRESS,
                    token0_decimals,
                    token1_decimals
                )
                
                # Get WBNB-BUSD prices
                wbnb_busd_price, _ = await price_fetcher.get_token_price(
                    WBNB_BUSD_POOL_ADDRESS,
                    18,  # WBNB decimals
                    18   # BUSD decimals
                )
                
                # Calculate NEI price in BUSD
                nei_busd_price = nei_wbnb_price * wbnb_busd_price
                return web.Response(text=str(nei_busd_price))
            except Exception as e:
                return web.Response(status=500, text=str(e))

    app = web.Application()
    app.router.add_get('/', get_price_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 8080)
    
    logger.info("Starting price server at http://localhost:8080")
    await site.start()
    
    try:
        # Keep the server running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down price server...")
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main()) 