
import asyncio
import json
import logging
from binance import AsyncClient, BinanceSocketManager

# Configure logging (basic configuration, level will be set by main.py)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class TradingEngine:
    def __init__(self, api_key, api_secret, testnet=False, strategy_config=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.strategy_config = strategy_config if strategy_config else {}
        self.client = None
        self.bsm = None
        self.market_data = {}
        self.positions = {}
        self.orders = {}
        self.initialized = False
        self.socket_tasks = [] # To keep track of running socket tasks

    async def initialize(self):
        logging.info("Initializing Binance client...")
        self.client = await AsyncClient.create(self.api_key, self.api_secret, testnet=self.testnet)
        self.bsm = BinanceSocketManager(self.client)
        logging.info("Binance client initialized.")
        await self._load_initial_data()
        self.initialized = True

    async def _load_initial_data(self):
        logging.info("Loading initial account and market data...")
        try:
            # Get account balance
            account_info = await self.client.futures_account_balance()
            quote_asset = self.strategy_config.get("quote_asset", "USDT")
            for asset_data in account_info:
                if asset_data["asset"] == quote_asset:
                    logging.info(f"Initial balance: {asset_data["balance"]} {asset_data["asset"]}")
                    break

            # Get open positions using futures_position_information()
            positions_info = await self.client.futures_position_information()
            for pos in positions_info:
                if float(pos["positionAmt"]) != 0:
                    self.positions[pos["symbol"]] = pos
            logging.info(f"Loaded {len(self.positions)} open positions.")

            # Get market info for symbols in strategy
            if "markets" in self.strategy_config:
                for symbol in self.strategy_config["markets"]:
                    ticker = await self.client.futures_symbol_ticker(symbol=symbol)
                    self.market_data[symbol] = {"price": float(ticker["price"])} # Simplified for now
                    logging.info(f"Loaded initial price for {symbol}: {ticker["price"]}")

        except Exception as e:
            logging.error(f"Error loading initial data: {e}")
            raise

    async def start_websocket_streams(self):
        if not self.initialized:
            await self.initialize()

        logging.info("Starting WebSocket streams...")
        
        # User data stream for account updates (positions, orders, balance)
        user_data_socket = self.bsm.futures_user_socket()
        self.socket_tasks.append(asyncio.create_task(self._start_socket(user_data_socket, self._handle_user_data)))

        # Mark price stream for each market in strategy
        if "markets" in self.strategy_config:
            for symbol in self.strategy_config["markets"]:
                mark_price_socket = self.bsm.futures_mark_price_socket(symbol=symbol)
                self.socket_tasks.append(asyncio.create_task(self._start_socket(mark_price_socket, self._handle_mark_price)))
        
        logging.info("WebSocket streams started.")

    async def _start_socket(self, socket, handler):
        async with socket as s:
            while True:
                msg = await s.recv()
                if msg:
                    await handler(msg)

    async def _handle_user_data(self, msg):
        # logging.debug(f"User Data: {msg}")
        if msg["e"] == "ACCOUNT_UPDATE":
            self._process_account_update(msg["a"])
        elif msg["e"] == "ORDER_TRADE_UPDATE":
            self._process_order_trade_update(msg["o"])

    async def _handle_mark_price(self, msg):
        # logging.debug(f"Mark Price: {msg}")
        symbol = msg["s"]
        self.market_data[symbol]["price"] = float(msg["p"])
        await self._execute_strategy(symbol)

    def _process_account_update(self, account_data):
        # Update balances
        for balance_info in account_data.get("B", []):
            asset = balance_info["a"]
            wallet_balance = float(balance_info["wb"])
            # Update self.balances if needed
            # logging.info(f"Balance update for {asset}: {wallet_balance}")

        # Update positions
        for position_info in account_data.get("P", []):
            symbol = position_info["s"]
            position_amt = float(position_info["pa"])
            entry_price = float(position_info["ep"])
            unrealized_pnl = float(position_info["up"])
            
            if position_amt != 0:
                self.positions[symbol] = {
                    "symbol": symbol,
                    "positionAmt": position_amt,
                    "entryPrice": entry_price,
                    "unRealizedProfit": unrealized_pnl
                }
                # logging.info(f"Position update for {symbol}: Amt={position_amt}, Entry={entry_price}, PnL={unrealized_pnl}")
            elif symbol in self.positions:
                del self.positions[symbol] # Position closed
                # logging.info(f"Position for {symbol} closed.")

    def _process_order_trade_update(self, order_data):
        order_status = order_data["X"]
        symbol = order_data["s"]
        order_id = order_data["i"]
        
        # logging.info(f"Order update for {symbol} (ID: {order_id}): Status={order_status}")
        self.orders[order_id] = order_data
        # Further processing based on order status (e.g., filled, canceled)

    async def _execute_strategy(self, symbol):
        # This is where the strategy logic will be applied.
        # For now, it's a placeholder.
        current_price = self.market_data[symbol]["price"]
        # logging.info(f"Executing strategy for {symbol} at price {current_price}")

        # Example: Simple logic to place a test order if no open positions
        if symbol not in self.positions and self.strategy_config.get("place_test_orders", False):
            if not self.strategy_config.get("markets"):
                logging.warning("No markets defined in strategy config. Cannot place test orders.")
                return

            if symbol not in self.strategy_config["markets"]:
                return # Only trade configured markets

            # Example: Place a small buy order
            try:
                quantity = self.strategy_config.get("test_order_quantity", 0.001)
                # Ensure quantity is within limits and step size
                # For a real bot, you'd calculate quantity based on risk, balance, etc.

                order = await self.client.futures_create_order(
                    symbol=symbol,
                    side="BUY",
                    type="MARKET",
                    quantity=quantity
                )
                logging.info(f"Placed test BUY order for {symbol}: {order}")
                self.orders[order["orderId"]] = order
            except Exception as e:
                logging.error(f"Error placing test order for {symbol}: {e}")

    async def stop(self):
        logging.info("Stopping TradingEngine...")
        # Cancel all running socket tasks
        for task in self.socket_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task # Await to ensure cancellation is processed
                except asyncio.CancelledError:
                    pass
        
        if self.client:
            await self.client.close_connection()
        logging.info("TradingEngine stopped.")

    async def run(self):
        try:
            await self.initialize()
            await self.start_websocket_streams()
            # Keep the event loop running indefinitely until stopped
            while True:
                await asyncio.sleep(1) # Small sleep to prevent busy-waiting
        except Exception as e:
            logging.critical(f"TradingEngine encountered an error: {e}")
        finally:
            await self.stop()

if __name__ == '__main__':
    # This block is for testing the engine independently.
    # In a real scenario, main.py will instantiate and run the engine.
    # Replace with your actual API keys or environment variables for testing
    API_KEY = os.getenv("BINANCE_API_KEY", "YOUR_API_KEY_FOR_TESTING")
    API_SECRET = os.getenv("BINANCE_API_SECRET", "YOUR_API_SECRET_FOR_TESTING")
    TESTNET = True # Set to True for testnet, False for mainnet

    # Example strategy config for testing
    test_strategy_config = {
        "markets": ["BTCUSDT", "ETHUSDT"],
        "place_test_orders": False, # Set to True to enable placing test orders
        "test_order_quantity": 0.001,
        "quote_asset": "USDT"
    }

    async def main_test():
        engine = TradingEngine(API_KEY, API_SECRET, testnet=TESTNET, strategy_config=test_strategy_config)
        await engine.run()

    try:
        asyncio.run(main_test())
    except KeyboardInterrupt:
        logging.info("TradingEngine stopped by user.")

