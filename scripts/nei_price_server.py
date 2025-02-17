#!/usr/bin/env python3

import asyncio
import logging
from aiohttp import web
from aiohttp.web import middleware
from pancakeswap_price_fetcher import PancakeSwapPriceFetcher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global price fetcher instance
price_fetcher = None
last_price = None
last_update_time = 0

@middleware
async def cors_middleware(request, handler):
    """Handle CORS headers."""
    response = await handler(request)
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

async def get_price():
    """Fetch NEI price in USDT with caching."""
    global price_fetcher, last_price, last_update_time
    
    current_time = asyncio.get_event_loop().time()
    
    # Use cached price if less than 5 seconds old
    if last_price is not None and current_time - last_update_time < 5:
        return last_price
    
    if price_fetcher is None:
        price_fetcher = PancakeSwapPriceFetcher()
        await price_fetcher.__aenter__()
    
    try:
        # Get NEI-WBNB prices
        nei_wbnb_price, _ = await price_fetcher.get_token_price(
            "0x96e48ce48dce021796cb3cd8864226a8e64e1f7d",  # NEI-WBNB pool
            18,  # NEI decimals
            18   # WBNB decimals
        )
        
        # Get WBNB-USDT prices (using BUSD as they're typically 1:1)
        wbnb_busd_price, _ = await price_fetcher.get_token_price(
            "0x58F876857a02D6762E0101bb5C46A8c1ED44Dc16",  # WBNB-BUSD pool
            18,  # WBNB decimals
            18   # BUSD decimals
        )
        
        # Calculate NEI price in USDT (using BUSD price as they're typically 1:1)
        nei_usdt_price = nei_wbnb_price * wbnb_busd_price
        
        # Update cache
        last_price = str(nei_usdt_price)
        last_update_time = current_time
        
        return last_price
        
    except Exception as e:
        logger.error(f"Error fetching price: {str(e)}")
        if last_price is not None:
            logger.info("Using last known price due to error")
            return last_price
        raise

async def price_handler(request):
    """Handle price requests."""
    try:
        price = await get_price()
        return web.Response(text=price)
    except Exception as e:
        return web.Response(status=500, text=str(e))

async def health_check(request):
    """Health check endpoint."""
    return web.Response(text="OK")

async def main():
    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get('/', price_handler)
    app.router.add_get('/health', health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    # Try different ports if 9000 is in use
    ports = [9000, 9001, 9002, 9003]
    site = None
    
    for port in ports:
        try:
            site = web.TCPSite(runner, 'localhost', port)
            await site.start()
            logger.info(f"Starting NEI price server at http://localhost:{port}")
            break
        except OSError:
            logger.warning(f"Port {port} is in use, trying next port...")
            continue
    
    if site is None:
        logger.error("Could not find an available port")
        return
        
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down price server...")
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main()) 