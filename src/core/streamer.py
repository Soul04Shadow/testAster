import asyncio
import json
import ssl
import websockets
from src.utils.config import config
from src.database.db import insert_liquidation, get_db_conn, get_usdt_volume_in_window, get_volume_in_window
from src.utils.utils import log
from src.core.order_batcher import LiquidationBuffer

class LiquidationStreamer:
    def __init__(self, message_handler):
        self.ws_url = config.WS_URL
        self.stream = config.LIQUIDATION_STREAM
        self.message_handler = message_handler
        # Database connection no longer stored - use fresh connections instead

        # Initialize liquidation buffer for batch processing
        buffer_window_ms = config.GLOBAL_SETTINGS.get('liquidation_buffer_ms', 100)
        self.liquidation_buffer = LiquidationBuffer(buffer_window_ms=buffer_window_ms)
        self.batch_processor_task = None

    async def subscribe(self, websocket):
        """Send subscription message to include the stream."""
        subscribe_msg = {
            "method": "SUBSCRIBE",
            "params": [self.stream],
            "id": 1
        }
        await websocket.send(json.dumps(subscribe_msg))
        log.info(f"Subscribed to {self.stream}")

    async def listen(self):
        """Connect to websocket and listen for messages."""
        # Create SSL context that handles certificate verification properly
        ssl_context = ssl.create_default_context()
        
        # Allow disabling SSL verification via config
        disable_ssl_verify = config.GLOBAL_SETTINGS.get('disable_ssl_verify', True)
        if disable_ssl_verify:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            log.warning("SSL certificate verification disabled for WebSocket connection")
        
        while True:
            try:
                uri = f"{self.ws_url}?streams={self.stream}"
                async with websockets.connect(uri, ssl=ssl_context) as websocket:
                    log.info("Connected to websocket")
                    await self.subscribe(websocket)
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            if 'data' in data:  # Wrapped stream
                                payload = data['data']
                            else:
                                payload = data

                            if payload.get('e') == 'forceOrder':
                                await self.process_liquidation(payload)
                        except json.JSONDecodeError as e:
                            log.error(f"Failed to parse message: {e}")
                        except Exception as e:
                            log.error(f"Error processing message: {e}")
            except websockets.exceptions.ConnectionClosedError:
                log.warning("WebSocket connection closed, reconnecting...")
                await asyncio.sleep(5)  # Reconnect delay
            except Exception as e:
                log.error(f"WebSocket error: {e}, reconnecting...")
                await asyncio.sleep(5)

    async def process_liquidation(self, payload):
        """Process a liquidation event and insert into DB."""
        liquidation = payload['o']  # The order object
        symbol = liquidation['s']
        side = liquidation['S']
        qty = float(liquidation['q'])
        price = float(liquidation['p']) if liquidation['p'] != '0' else 0.0  #Avg price or 0
        usdt_value = qty * price  # Calculate USDT value

        # Use fresh database connection
        conn = get_db_conn()
        insert_liquidation(conn, symbol, side, qty, price)
        conn.commit()

        # Get volume tracking info if symbol is configured
        volume_info = ""
        if symbol in config.SYMBOLS:
            # Get current tracked volume
            use_usdt_volume = config.GLOBAL_SETTINGS.get('use_usdt_volume', False)
            window_sec = config.GLOBAL_SETTINGS.get('volume_window_sec', 60)

            if use_usdt_volume:
                current_volume = get_usdt_volume_in_window(conn, symbol, window_sec)
                volume_type = "USDT"
            else:
                current_volume = get_volume_in_window(conn, symbol, window_sec)
                volume_type = "tokens"

            # Get symbol settings
            symbol_config = config.SYMBOL_SETTINGS.get(symbol, {})

            # Determine which threshold applies (opposite to liquidation side)
            if side == "SELL":  # Long liquidation -> would open LONG position
                threshold = symbol_config.get('volume_threshold_long',
                                             symbol_config.get('volume_threshold', 10000))
                threshold_type = "LONG"
            else:  # Short liquidation (BUY) -> would open SHORT position
                threshold = symbol_config.get('volume_threshold_short',
                                             symbol_config.get('volume_threshold', 10000))
                threshold_type = "SHORT"

            # Calculate percentage
            percentage = (current_volume / threshold * 100) if threshold > 0 else 0

            # Format volume info
            volume_info = f" | Volume: {current_volume:,.0f}/{threshold:,.0f} {volume_type} ({percentage:.0f}% to {threshold_type} threshold)"

        conn.close()

        # Log liquidation with color coding and volume info
        log.liquidation(symbol, side, qty, price, usdt_value, volume_info)

        # Add to buffer for batch processing if enabled
        if config.GLOBAL_SETTINGS.get('buffer_liquidations', True):
            self.liquidation_buffer.add_liquidation(symbol, side, qty, price)

            # Process batch if ready
            batch = self.liquidation_buffer.get_batch()
            if batch:
                await self.process_liquidation_batch(batch)
        else:
            # Pass to message handler directly (no batching)
            if self.message_handler:
                await self.message_handler(symbol, side, qty, price)

    async def process_liquidation_batch(self, batch):
        """Process a batch of liquidations."""
        log.debug(f"Processing batch of {len(batch)} liquidations")

        # Process each liquidation in the batch
        tasks = []
        for liq in batch:
            if self.message_handler:
                tasks.append(self.message_handler(liq['symbol'], liq['side'], liq['qty'], liq['price']))

        # Process all liquidations concurrently
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
