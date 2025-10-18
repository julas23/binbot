
import asyncio
import json
import logging
import os
from engine import TradingEngine

# Configure logging (default to INFO, can be overridden by config.json)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def main():
    logging.info("Starting Binance Trading Bot...")

    # Load main configuration from config.json
    try:
        with open("config.json", "r") as f:
            main_config = json.load(f)
        logging.info("Main configuration loaded successfully from config.json")
    except FileNotFoundError:
        logging.error("config.json not found. Please create one with API keys and other settings.")
        return
    except json.JSONDecodeError:
        logging.error("Invalid JSON in config.json. Please check its format.")
        return

    # Set logging level from config.json if available
    if 'LOG_LEVEL' in main_config:
        logging.getLogger().setLevel(main_config['LOG_LEVEL'].upper())

    # Load strategy from strategy.json
    try:
        with open("strategy.json", "r") as f:
            strategy_config = json.load(f)
        logging.info("Strategy loaded successfully from strategy.json")
    except FileNotFoundError:
        logging.error("strategy.json not found. Please create one.")
        return
    except json.JSONDecodeError:
        logging.error("Invalid JSON in strategy.json. Please check its format.")
        return

    # Get API keys, prioritizing environment variables for security
    api_key = os.getenv("BINANCE_API_KEY", main_config.get("BINANCE_API_KEY", ""))
    api_secret = os.getenv("BINANCE_API_SECRET", main_config.get("BINANCE_API_SECRET", ""))
    use_testnet = main_config.get("USE_TESTNET", True) # Default to True for safety

    # Validate essential API keys
    if not api_key or not api_secret:
        logging.error("Binance API Key or Secret not found. Please set them in config.json or as environment variables.")
        return

    # Log API key and secret (partially masked for security)
    logging.info(f"Using API Key: {api_key[:5]}...{api_key[-5:]}")
    logging.info(f"Using API Secret: {api_secret[:5]}...{api_secret[-5:]}")
    logging.info(f"Using Testnet: {use_testnet}")

    engine = TradingEngine(
        api_key=api_key,
        api_secret=api_secret,
        testnet=use_testnet,
        strategy_config=strategy_config
    )

    try:
        await engine.run()
    except Exception as e:
        logging.critical(f"An unhandled error occurred in the trading engine: {e}")
    finally:
        await engine.stop()
        logging.info("Binance Trading Bot stopped.")

if __name__ == "__main__":
    asyncio.run(main())