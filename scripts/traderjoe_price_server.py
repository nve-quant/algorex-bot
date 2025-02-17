#!/usr/bin/env python3

import asyncio
import logging
from aiohttp import web
from aiohttp.web import middleware
from traderjoe_price_fetcher import TraderJoePriceFetcher

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

# Token addresses
LOD3_ADDRESS = "0xbbaaa0420d474b34be197f95a323c2ff3829e811"  # LOD3 token
USDC_ADDRESS = "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"  # USDC.e on Avalanche

@middleware
async def cors_middleware(request, handler):
    """Handle CORS headers."""
    response = await handler(request)
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

async def get_price():
    """Fetch LOD3 price in USDT with caching."""
    global price_fetcher, last_price, last_update_time
    
    current_time = asyncio.get_event_loop().time()
    
    # Use cached price if less than 5 seconds old
    if last_price is not None and current_time - last_update_time < 5:
        logger.info(f"Using cached price: {last_price}")
        return last_price
    
    if price_fetcher is None:
        logger.info("Initializing price fetcher")
        price_fetcher = TraderJoePriceFetcher()
        await price_fetcher.__aenter__()
    
    try:
        logger.info("Starting price calculation")
        
        # Get LOD3/USDC.e price (which is effectively the same as USDT since USDC.e:USDT is ~1:1)
        logger.info("Getting LOD3/USDC.e price...")
        lod3_usdc_price, lod3_usdc_volume = await price_fetcher.get_token_price(
            LOD3_ADDRESS,  # LOD3 token
            18,  # LOD3 decimals
            6    # USDC decimals
        )
        logger.info(f"LOD3/USDC.e price: {lod3_usdc_price:.6f} USDC.e per LOD3")
        logger.info(f"LOD3/USDC.e volume: {lod3_usdc_volume:.6f} USDC.e")
        
        # Use USDC.e price as USDT price (they're pegged 1:1)
        lod3_usdt_price = lod3_usdc_price
        logger.info(f"Final LOD3/USDT price: {lod3_usdt_price:.6f} USDT per LOD3")
        
        # Update cache
        last_price = str(lod3_usdt_price)
        last_update_time = current_time
        
        return last_price
        
    except Exception as e:
        logger.error(f"Error fetching price: {str(e)}", exc_info=True)
        if last_price is not None:
            logger.info(f"Using last known price due to error: {last_price}")
            return last_price
        raise

async def price_handler(request):
    """Handle price requests."""
    try:
        price = await get_price()
        return web.Response(text=price)
    except Exception as e:
        error_msg = f"Error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return web.Response(status=500, text=error_msg)

async def health_check(request):
    """Health check endpoint."""
    return web.Response(text="OK")

async def main():
    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get('/', price_handler)
    app.router.add_get('/health', health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    # Try different ports if 9010 is in use (using different port range than NEI server)
    ports = [9010, 9011, 9012, 9013]
    site = None
    
    for port in ports:
        try:
            site = web.TCPSite(runner, 'localhost', port)
            await site.start()
            logger.info(f"Starting TraderJoe price server at http://localhost:{port}")
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