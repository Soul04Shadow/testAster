"""
WebSocket user data stream for real-time order and position updates.
"""

import asyncio
import websockets
import json
import ssl
import time
import logging
from typing import Optional, Callable
from src.utils.auth import make_authenticated_request
from src.utils.config import config

logger = logging.getLogger(__name__)


class UserDataStream:
    """
    Manages WebSocket connection for user data stream.
    Receives real-time updates for orders, positions, and account changes.
    """

    def __init__(self, order_manager=None, position_manager=None, db_conn=None, order_cleanup=None):
        """
        Initialize user data stream.

        Args:
            order_manager: OrderManager instance for order updates
            position_manager: PositionManager instance for position updates
            db_conn: Database connection for persistence
            order_cleanup: OrderCleanup instance for cleaning orphaned orders
        """
        self.order_manager = order_manager
        self.position_manager = position_manager
        # Use config DB_PATH consistently
        self.db_path = config.DB_PATH
        self.order_cleanup = order_cleanup

        self.ws_url = "wss://fstream.asterdex.com/ws/"
        self.listen_key = None
        self.ws = None
        self.running = False
        self.keepalive_task = None

        logger.info("User data stream initialized")

    async def create_listen_key(self) -> Optional[str]:
        """
        Create a listen key for user data stream.

        Returns:
            Listen key string or None
        """
        try:
            response = make_authenticated_request(
                'POST',
                f"{config.BASE_URL}/fapi/v1/listenKey"
            )

            if response.status_code == 200:
                data = response.json()
                listen_key = data.get('listenKey')
                logger.info(f"Created listen key: {listen_key[:8]}...")
                return listen_key
            else:
                logger.error(f"Failed to create listen key: {response.text}")
        except Exception as e:
            logger.error(f"Error creating listen key: {e}")

        return None

    async def keepalive_listen_key(self) -> bool:
        """
        Keepalive the listen key to prevent expiration.

        Returns:
            True if successful
        """
        if not self.listen_key:
            return False

        try:
            response = make_authenticated_request(
                'PUT',
                f"{config.BASE_URL}/fapi/v1/listenKey"
            )

            if response.status_code == 200:
                logger.debug("Listen key keepalive successful")
                return True
            else:
                logger.error(f"Listen key keepalive failed: {response.text}")
        except Exception as e:
            logger.error(f"Error in listen key keepalive: {e}")

        return False

    async def close_listen_key(self) -> None:
        """Close the listen key."""
        if not self.listen_key:
            return

        try:
            response = make_authenticated_request(
                'DELETE',
                f"{config.BASE_URL}/fapi/v1/listenKey"
            )

            if response.status_code == 200:
                logger.info("Listen key closed")
            else:
                logger.error(f"Failed to close listen key: {response.text}")
        except Exception as e:
            logger.error(f"Error closing listen key: {e}")

    async def keepalive_loop(self) -> None:
        """
        Keepalive loop to maintain listen key.
        Sends keepalive every 30 minutes.
        """
        while self.running:
            try:
                await asyncio.sleep(1800)  # 30 minutes
                if self.running:
                    await self.keepalive_listen_key()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in keepalive loop: {e}")

    async def handle_account_update(self, data: dict) -> None:
        """
        Handle account update event.

        Args:
            data: Account update data
        """
        event_time = data.get('E', 0)
        balances = data.get('a', {}).get('B', [])

        for balance in balances:
            asset = balance.get('a')
            wallet_balance = float(balance.get('wb', 0))
            cross_wallet = float(balance.get('cw', 0))

            logger.info(f"Balance update - {asset}: wallet={wallet_balance}, cross={cross_wallet}")

    async def handle_order_update(self, data: dict) -> None:
        """
        Handle order update event.

        Args:
            data: Order update data
        """
        order_data = data.get('o', {})

        symbol = order_data.get('s')
        order_id = str(order_data.get('i'))
        side = order_data.get('S')
        order_type = order_data.get('o')
        status = order_data.get('X')
        price = float(order_data.get('p', 0))
        quantity = float(order_data.get('q', 0))
        filled_qty = float(order_data.get('z', 0))
        position_side = order_data.get('ps', 'BOTH')

        # Extract trade-specific fields for fills
        trade_id = order_data.get('t', 0)  # Trade ID
        avg_price = float(order_data.get('ap', 0))  # Average Price
        realized_pnl = float(order_data.get('rp', 0))  # Realized Profit
        commission_amount = float(order_data.get('n', 0)) if 'n' in order_data else None  # Commission
        commission_asset = order_data.get('N', 'USDT')  # Commission Asset

        logger.info(f"Order update - {order_id}: {symbol} {side} {status} (filled: {filled_qty}/{quantity})")

        # Log trade details if this is a fill
        if trade_id and trade_id != 0:
            logger.info(f"Trade executed - ID: {trade_id}, Avg Price: {avg_price}, Realized PnL: {realized_pnl}, Commission: {commission_amount} {commission_asset}")

        # Update order manager
        if self.order_manager:
            self.order_manager.update_order_status(order_id, status, filled_qty)

        # Update database
        if self.db_path:
            try:
                # Import the functions we need
                from src.database.db import update_trade_on_fill, insert_order_status, update_order_filled, update_order_canceled
                use_new_db = True

                if use_new_db:
                    # Use the new update_trade_on_fill function
                    if status in ['FILLED', 'PARTIALLY_FILLED']:
                        import sqlite3
                        conn = sqlite3.connect(self.db_path)
                        # Update trade with fill information
                        rows_updated = update_trade_on_fill(
                        conn,
                        order_id=order_id,
                        trade_id=trade_id,
                        status=status,
                        filled_qty=filled_qty,
                        avg_price=avg_price if avg_price > 0 else price,
                        realized_pnl=realized_pnl,
                        commission=-abs(commission_amount) if commission_amount else None  # Store as negative
                        )

                        if rows_updated == 0:
                            logger.warning(f"No trade record found for order {order_id}, may need to create one")
                            # If no existing trade, we might need to insert it
                            # This can happen if the order was placed before our tracking started
                            from src.database.db import insert_trade
                            insert_trade(
                            conn,
                            symbol=symbol,
                            order_id=order_id,
                            side=side,
                            qty=quantity,
                            price=avg_price if avg_price > 0 else price,
                            status=status,
                            order_type=order_type
                            )
                            # Then update with fill details
                            update_trade_on_fill(
                            conn,
                            order_id=order_id,
                            trade_id=trade_id,
                            status=status,
                            filled_qty=filled_qty,
                            avg_price=avg_price if avg_price > 0 else price,
                            realized_pnl=realized_pnl,
                            commission=-abs(commission_amount) if commission_amount else None
                            )
                        conn.commit()
                        conn.close()

                    elif status == 'CANCELED':
                        # Just update status to canceled
                        import sqlite3
                        conn = sqlite3.connect(self.db_path)
                        cursor = conn.cursor()
                        cursor.execute('UPDATE trades SET status = ? WHERE order_id = ?', ('CANCELED', order_id))
                        conn.commit()
                        conn.close()

                else:
                    # Fallback to old db_updated methods
                    import sqlite3
                    conn = sqlite3.connect(self.db_path)
                    if status == 'FILLED':
                        update_order_filled(conn, order_id, filled_qty)
                    elif status == 'CANCELED':
                        update_order_canceled(conn, order_id)
                    else:
                        insert_order_status(conn, order_id, symbol, side, quantity, price, position_side, status)
                    conn.commit()
                    conn.close()
            except Exception as e:
                logger.error(f"Error updating database for order {order_id}: {e}")
                # Continue processing even if database update fails

        # Update position manager when order fills
        if status == 'FILLED' and self.position_manager:
            # Remove pending exposure
            self.position_manager.remove_pending_exposure(symbol, filled_qty * price)

            # Update actual position
            position_side_mapped = 'LONG' if (side == 'BUY' and position_side != 'SHORT') or position_side == 'LONG' else 'SHORT'
            self.position_manager.update_position(symbol, position_side_mapped, filled_qty, price)

    async def handle_position_update(self, data: dict) -> None:
        """
        Handle position update event from ACCOUNT_UPDATE.

        Args:
            data: Position update data
        """
        positions = data.get('a', {}).get('P', [])

        for pos_data in positions:
            symbol = pos_data.get('s')
            position_amount = float(pos_data.get('pa', 0))
            entry_price = float(pos_data.get('ep', 0))
            unrealized_pnl = float(pos_data.get('up', 0))
            position_side = pos_data.get('ps', 'BOTH')

            if position_amount != 0:
                logger.info(f"Position update - {symbol} {position_side}: {position_amount}@{entry_price}, PnL={unrealized_pnl}")

                # Update position manager
                if self.position_manager:
                    side = 'LONG' if position_amount > 0 else 'SHORT'
                    self.position_manager.update_position(symbol, side, abs(position_amount), entry_price)

                # Update database
                if self.db_path:
                    try:
                        from src.database.db import insert_or_update_position
                        import sqlite3
                        conn = sqlite3.connect(self.db_path)
                        side = 'LONG' if position_amount > 0 else 'SHORT'
                        insert_or_update_position(conn, symbol, side, abs(position_amount), entry_price, entry_price)
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        logger.error(f"Error updating position for {symbol}: {e}")
            else:
                # Position closed
                logger.info(f"Position closed for {symbol}")

                if self.position_manager:
                    self.position_manager.close_position(symbol)

                if self.db_path:
                    try:
                        from src.database.db import delete_position
                        import sqlite3
                        conn = sqlite3.connect(self.db_path)
                        delete_position(conn, symbol)
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        logger.error(f"Error deleting position for {symbol}: {e}")

                # Cleanup orphaned TP/SL orders when position closes
                if self.order_cleanup:
                    logger.info(f"Triggering order cleanup for closed position {symbol}")
                    asyncio.create_task(self.order_cleanup.cleanup_on_position_close(symbol))

    async def handle_message(self, message: str) -> None:
        """
        Handle incoming WebSocket message.

        Args:
            message: Raw message string
        """
        try:
            data = json.loads(message)
            event_type = data.get('e')

            if event_type == 'ACCOUNT_UPDATE':
                await self.handle_account_update(data)
                await self.handle_position_update(data)

            elif event_type == 'ORDER_TRADE_UPDATE':
                await self.handle_order_update(data)

            elif event_type == 'listenKeyExpired':
                logger.warning("Listen key expired, reconnecting...")
                await self.reconnect()

            elif event_type == 'MARGIN_CALL':
                logger.error(f"MARGIN CALL received: {data}")

            else:
                logger.debug(f"Unhandled event type: {event_type}")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode message: {e}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def connect(self) -> None:
        """Connect to user data stream."""
        # Create listen key
        self.listen_key = await self.create_listen_key()
        if not self.listen_key:
            logger.error("Failed to create listen key")
            return

        # Connect to WebSocket
        ws_url = f"{self.ws_url}{self.listen_key}"

        try:
            # Create SSL context that handles certificate verification properly
            ssl_context = ssl.create_default_context()
            
            # Allow disabling SSL verification via config (same as liquidation stream)
            disable_ssl_verify = config.GLOBAL_SETTINGS.get('disable_ssl_verify', True)
            if disable_ssl_verify:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                logger.warning("SSL certificate verification disabled for user data stream")
            
            self.ws = await websockets.connect(ws_url, ssl=ssl_context)
            logger.info("Connected to user data stream")

            # Start keepalive task
            self.keepalive_task = asyncio.create_task(self.keepalive_loop())

            # Listen for messages
            async for message in self.ws:
                if not self.running:
                    break
                await self.handle_message(message)

        except websockets.exceptions.ConnectionClosed:
            logger.warning("User data stream connection closed")
            if self.running:
                await self.reconnect()

        except Exception as e:
            logger.error(f"Error in user data stream: {e}")
            if self.running:
                await asyncio.sleep(5)
                await self.reconnect()

    async def reconnect(self) -> None:
        """Reconnect to user data stream."""
        logger.info("Reconnecting user data stream...")

        # Clean up existing connection
        if self.ws:
            await self.ws.close()
            self.ws = None

        if self.keepalive_task:
            self.keepalive_task.cancel()
            self.keepalive_task = None

        # Close old listen key
        if self.listen_key:
            await self.close_listen_key()
            self.listen_key = None

        # Wait before reconnecting
        await asyncio.sleep(5)

        # Reconnect
        if self.running:
            await self.connect()

    async def start(self) -> None:
        """Start the user data stream."""
        self.running = True
        await self.connect()

    async def stop(self) -> None:
        """Stop the user data stream."""
        logger.info("Stopping user data stream...")
        self.running = False

        # Cancel keepalive
        if self.keepalive_task:
            self.keepalive_task.cancel()
            try:
                await self.keepalive_task
            except asyncio.CancelledError:
                pass
            self.keepalive_task = None

        # Close WebSocket with timeout
        if self.ws:
            try:
                await asyncio.wait_for(self.ws.close(), timeout=2.0)
            except asyncio.TimeoutError:
                logger.warning("WebSocket close timed out")
            except Exception as e:
                logger.warning(f"Error closing WebSocket: {e}")
            self.ws = None

        # Close listen key
        if self.listen_key:
            try:
                await self.close_listen_key()
            except Exception as e:
                logger.warning(f"Error closing listen key: {e}")
            self.listen_key = None

        logger.info("User data stream stopped")
