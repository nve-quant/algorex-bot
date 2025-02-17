#!/usr/bin/env python3

import json
import logging
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey
import base64
import struct

logger = logging.getLogger(__name__)

class RaydiumPriceFetcher:
    """Price fetcher for Raydium on Solana."""
    
    # Known pool addresses
    LOD3_USDC_POOL = "7Qc3GDyJuVpnk1Y8iCujdgS6TqbvCQVwg3kYheB6Ho8c"  # LOD3-USDC pool
    
    def __init__(self, rpc_url: str = "https://api.mainnet-beta.solana.com"):
        """Initialize with Solana RPC URL."""
        self.client = AsyncClient(rpc_url)

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.client.close()

    async def get_price_from_pool(self, pool_address: str, token_decimals: int, quote_decimals: int) -> tuple[float, float]:
        """
        Get token price from a Raydium pool.
        
        Args:
            pool_address: Address of the pool
            token_decimals: Decimals of the token to price
            quote_decimals: Decimals of the quote token
            
        Returns:
            Tuple of (price, volume in quote currency)
        """
        logger.info(f"Getting price from pool {pool_address}")
        
        try:
            # Get account data
            response = await self.client.get_account_info(Pubkey.from_string(pool_address))
            if not response.value:
                raise ValueError(f"Pool {pool_address} not found")
            
            data = base64.b64decode(response.value.data[0])
            
            # Raydium AMM account layout:
            # - status: u64 (8 bytes)
            # - nonce: u64 (8 bytes)
            # - orderNum: u64 (8 bytes)
            # - depth: u64 (8 bytes)
            # - coinDecimals: u64 (8 bytes)
            # - pcDecimals: u64 (8 bytes)
            # - state: u64 (8 bytes)
            # - resetFlag: u64 (8 bytes)
            # - minSize: u64 (8 bytes)
            # - volMaxCutRatio: u64 (8 bytes)
            # - amountWaveRatio: u64 (8 bytes)
            # - coinLotSize: u64 (8 bytes)
            # - pcLotSize: u64 (8 bytes)
            # - minPriceMultiplier: u64 (8 bytes)
            # - maxPriceMultiplier: u64 (8 bytes)
            # - systemDecimalsValue: u64 (8 bytes)
            # - minSeparateNumerator: u64 (8 bytes)
            # - minSeparateDenominator: u64 (8 bytes)
            # - tradeFeeNumerator: u64 (8 bytes)
            # - tradeFeeDenominator: u64 (8 bytes)
            # - pnlNumerator: u64 (8 bytes)
            # - pnlDenominator: u64 (8 bytes)
            # - swapFeeNumerator: u64 (8 bytes)
            # - swapFeeDenominator: u64 (8 bytes)
            # - needTakePnlCoin: u64 (8 bytes)
            # - needTakePnlPc: u64 (8 bytes)
            # - totalPnlPc: u64 (8 bytes)
            # - totalPnlCoin: u64 (8 bytes)
            # - poolCoinTokenAccount: Pubkey (32 bytes)
            # - poolPcTokenAccount: Pubkey (32 bytes)
            # - coinMintAddress: Pubkey (32 bytes)
            # - pcMintAddress: Pubkey (32 bytes)
            # - lpMintAddress: Pubkey (32 bytes)
            # - ammOpenOrders: Pubkey (32 bytes)
            # - serumMarket: Pubkey (32 bytes)
            # - serumProgramId: Pubkey (32 bytes)
            # - ammTargetOrders: Pubkey (32 bytes)
            # - poolWithdrawQueue: Pubkey (32 bytes)
            # - poolTempLpTokenAccount: Pubkey (32 bytes)
            # - ammOwner: Pubkey (32 bytes)
            # - pnlOwner: Pubkey (32 bytes)
            # - coinTokenBalance: u64 (8 bytes)
            # - pcTokenBalance: u64 (8 bytes)
            
            # Extract token balances (last two u64s)
            offset = len(data) - 16  # Last 16 bytes contain the two u64s
            coin_balance, pc_balance = struct.unpack("<QQ", data[offset:offset+16])
            
            logger.info(f"Pool balances: coin={coin_balance}, pc={pc_balance}")
            
            if coin_balance == 0 or pc_balance == 0:
                raise ValueError(f"No liquidity in pool {pool_address}")
            
            # Calculate price (pc/coin)
            price = (pc_balance / 10**quote_decimals) / (coin_balance / 10**token_decimals)
            volume = pc_balance / 10**quote_decimals
            
            logger.info(f"Calculated price: {price}, volume: {volume}")
            return price, volume
            
        except Exception as e:
            logger.error(f"Error getting price from pool: {str(e)}")
            raise

    async def get_token_price(self, token_decimals: int, quote_decimals: int) -> tuple[float, float]:
        """
        Get LOD3 token price in USDC.
        
        Args:
            token_decimals: Decimals of the token to price
            quote_decimals: Decimals of the quote token
            
        Returns:
            Tuple of (price, volume in quote currency)
        """
        # Get LOD3/USDC price
        return await self.get_price_from_pool(self.LOD3_USDC_POOL, token_decimals, quote_decimals) 