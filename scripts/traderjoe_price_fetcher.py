#!/usr/bin/env python3

import json
import logging
from web3 import Web3
from typing import Tuple

logger = logging.getLogger(__name__)

class TraderJoePriceFetcher:
    """Price fetcher for TraderJoe V2.1 on Avalanche."""
    
    # TraderJoe V2.1 contracts
    FACTORY_ADDRESS = "0x8e42f2F4101563bF679975178e880FD87d3eFd4e"
    WAVAX_ADDRESS = "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"
    USDC_ADDRESS = "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"  # USDC.e
    
    # Known pool addresses
    LOD3_USDC_POOL = "0xA1AeB013A997A7E0e3B7679d4eD49b1df09b694B"  # LOD3-USDC.e pool
    
    # TraderJoe V2.1 LB Pair ABI (only what we need)
    PAIR_ABI = json.loads('''[
        {"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
        {"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
        {"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint128","name":"reserve0","type":"uint128"},{"internalType":"uint128","name":"reserve1","type":"uint128"}],"stateMutability":"view","type":"function"}
    ]''')

    def __init__(self, rpc_url: str = "https://api.avax.network/ext/bc/C/rpc"):
        """Initialize with Avalanche RPC URL."""
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        pass

    async def get_price_from_pool(self, pool_address: str, token_address: str, token_decimals: int, quote_decimals: int) -> Tuple[float, float]:
        """
        Get token price from a specific TraderJoe V2.1 pool.
        
        Args:
            pool_address: Address of the pool
            token_address: Address of the token we want the price for
            token_decimals: Decimals of the token
            quote_decimals: Decimals of the quote token
            
        Returns:
            Tuple of (price, volume in quote currency)
        """
        logger.info(f"Getting price from pool {pool_address} for token {token_address}")
        
        # Create contract instance for the pair
        pair_contract = self.w3.eth.contract(
            address=Web3.to_checksum_address(pool_address),
            abi=self.PAIR_ABI
        )
        
        # Get tokens to determine order
        token0 = pair_contract.functions.token0().call().lower()
        token1 = pair_contract.functions.token1().call().lower()
        logger.info(f"Pool tokens: token0={token0}, token1={token1}")
        
        is_token1 = token_address.lower() == token1
        logger.info(f"Target token is token{'1' if is_token1 else '0'}")
        
        # Get reserves
        reserves = pair_contract.functions.getReserves().call()
        logger.info(f"Global reserves: reserve0={reserves[0]}, reserve1={reserves[1]}")
        
        if reserves[0] == 0 or reserves[1] == 0:
            raise ValueError(f"No liquidity in pool {pool_address}")
        
        # Calculate price based on reserves
        if is_token1:
            # If we want token1's price, divide reserve0 by reserve1
            price = (reserves[0] / 10**quote_decimals) / (reserves[1] / 10**token_decimals)
            volume = reserves[0] / 10**quote_decimals
        else:
            # If we want token0's price, divide reserve1 by reserve0
            price = (reserves[1] / 10**quote_decimals) / (reserves[0] / 10**token_decimals)
            volume = reserves[1] / 10**quote_decimals
        
        logger.info(f"Calculated price: {price}, volume: {volume}")
        return price, volume

    async def get_token_price(self, token_address: str, token_decimals: int, quote_decimals: int) -> Tuple[float, float]:
        """
        Get token price from TraderJoe V2.1 LB pair.
        
        Args:
            token_address: Address of the token to price
            token_decimals: Decimals of the token to price
            quote_decimals: Decimals of the quote token
            
        Returns:
            Tuple of (price, volume in quote currency)
        """
        # Get LOD3/USDC.e price (USDC.e is effectively 1:1 with USDT)
        if token_address.lower() == "0xbbaaa0420d474b34be197f95a323c2ff3829e811".lower():  # LOD3
            return await self.get_price_from_pool(self.LOD3_USDC_POOL, token_address, token_decimals, quote_decimals)
        else:
            raise ValueError(f"No known pool for token {token_address}") 