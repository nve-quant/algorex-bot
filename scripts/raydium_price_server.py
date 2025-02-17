#!/usr/bin/env python3

import asyncio
import logging
from aiohttp import web
from aiohttp.web import middleware
from raydium_price_fetcher import RaydiumPriceFetcher

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
LOD3_ADDRESS = "7Qc3GDyJuVpnk1Y8iCujdgS6TqbvCQVwg3kYheB6Ho8c"  # LOD3 token
USDC_ADDRESS = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC on Solana

@middleware
async def cors_middleware(request, handler):
    """Handle CORS headers."""
    response = await handler(request)
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

async def get_price():
    """Fetch LOD3 price in USDC with caching."""
    global price_fetcher, last_price, last_update_time
    
    current_time = asyncio.get_event_loop().time()
    
    # Use cached price if less than 5 seconds old
    if last_price is not None and current_time - last_update_time < 5:
        logger.info(f"Using cached price: {last_price}")
        return last_price
    
    if price_fetcher is None:
        logger.info("Initializing price fetcher")
        price_fetcher = RaydiumPriceFetcher()
        await price_fetcher.__aenter__()
    
    try:
        logger.info("Starting price calculation")
        
        # Get LOD3/USDC price
        logger.info("Getting LOD3/USDC price...")
        lod3_usdc_price, lod3_usdc_volume = await price_fetcher.get_token_price(
            9,  # LOD3 decimals
            6   # USDC decimals
        )
        logger.info(f"LOD3/USDC price: {lod3_usdc_price:.6f} USDC per LOD3")
        logger.info(f"LOD3/USDC volume: {lod3_usdc_volume:.6f} USDC")
        
        # Update cache
        last_price = str(lod3_usdc_price)
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
    
    # Try different ports if 9020 is in use (using different port range than other servers)
    ports = [9020, 9021, 9022, 9023]
    site = None
    
    for port in ports:
        try:
            site = web.TCPSite(runner, 'localhost', port)
            await site.start()
            logger.info(f"Starting Raydium price server at http://localhost:{port}")
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