import ccxt
import asyncio
import ccxt.async_support as async_ccxt

from datetime import datetime, timedelta
import logging  # Import the logging module

class CryptoPollingService:
    def __init__(self):
        self.bybit = async_ccxt.bybit()
        self.coinbasepro = async_ccxt.coinbasepro()
        self.last_updated_time = datetime.utcnow()
        self.previous_satoshi_rates = {}
        self.messages_to_send = []

        # Configure logging for this class
        self.logger = logging.getLogger('CryptoPollingService')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    async def monitor_and_notify_satoshi_rate_changes(self):
        self.logger.info("Starting monitoring of Satoshi rates.")
        common_symbols = await self.get_common_symbols()

        if not common_symbols:
            self.logger.info("No common symbols found between exchanges.")
            return

        current_satoshi_rates = await self.calculate_satoshi_rates_for_symbols(common_symbols)
        self.logger.debug(f"Current Satoshi rates: {current_satoshi_rates}")

        for symbol in current_satoshi_rates:
            previous_rate = self.previous_satoshi_rates.get(symbol)
            if previous_rate is not None:
                rate_change = abs(current_satoshi_rates[symbol] - previous_rate) / previous_rate
                if rate_change > 0.0001 and self.is_within_1_hour():
                    message = f"SatoshiRate for {symbol} changed by more than 0.01% in the last hour: {current_satoshi_rates[symbol]} compared to {previous_rate}"
                    self.messages_to_send.append(message)
                    self.logger.info(message)

            self.previous_satoshi_rates[symbol] = current_satoshi_rates[symbol]
        self.last_updated_time = datetime.utcnow()
    async def calculate_satoshi_rates_for_symbols(self, symbols):
        satoshi_rates = {}
        for symbol in symbols:
            satoshi_rate = await self.calculate_satoshi_rate(symbol)
            satoshi_rates[symbol] = satoshi_rate
        return satoshi_rates
    async def check_for_trigger(self):
        # If there are messages in the queue, return the first one and remove it from the list
        if self.messages_to_send:
            return self.messages_to_send.pop(0)
        else:
            return None

    def is_within_1_hour(self):
        # Check if the current time is within one hour of the last updated time
        return (datetime.utcnow() - self.last_updated_time) <= timedelta(hours=1)

    def map_bybit_to_coinbase(self, symbol):
        if symbol.endswith('/USDT'):
            return symbol.replace('/USDT', '/USD')
        # Add more mappings if needed
        return symbol

    def normalize_symbol(self, symbol):
        # Split the symbol and take the first part (base currency)
        return symbol.split('/')[0]

    async def get_common_symbols(self):
        try:
            # Check if the exchanges support fetching tickers
            if not (self.bybit.has['fetchTickers'] and self.coinbasepro.has['fetchTickers']):
                self.logger.error("One or both exchanges do not support fetching multiple tickers")
                return []

            # Fetch tickers for all symbols from each exchange
            bybit_tickers = await self.bybit.fetch_tickers()
            coinbasepro_tickers = await self.coinbasepro.fetch_tickers()

            # Normalize and map symbols from Bybit
            symbols_bybit = {self.map_bybit_to_coinbase(self.normalize_symbol(s)) for s in bybit_tickers.keys()}
            symbols_coinbase = {self.normalize_symbol(s) for s in coinbasepro_tickers.keys()}

            # Find common symbols
            common_symbols = symbols_bybit.intersection(symbols_coinbase)
            self.logger.info(f"Common symbols: {common_symbols}")
            return list(common_symbols)

        except Exception as e:
            self.logger.error(f"Error getting common symbols: {e}")
            return []
    async def calculate_satoshi_rate(self, symbol):
        try:
            bybit_futures_price = await self.fetch_bybit_futures_price(symbol)
            coinbase_spot_price = await self.fetch_coinbase_spot_price(symbol)
            if bybit_futures_price is not None and coinbase_spot_price is not None:
                satoshi_rate = (coinbase_spot_price - bybit_futures_price) / bybit_futures_price * 100
                self.logger.debug(f"Satoshi rate for {symbol}: {satoshi_rate}")
                return satoshi_rate
        except Exception as ex:
            self.logger.error(f"Error calculating Satoshi rate for {symbol}: {ex}")
            return None

    async def fetch_bybit_futures_price(self, symbol):
        try:
            ticker = await self.bybit.fetch_ticker(symbol)
            self.logger.debug(f"Fetched Bybit futures price for {symbol}: {ticker['last']}")
            return ticker['last']
        except Exception as ex:
            self.logger.error(f"Error fetching Bybit futures price for {symbol}: {ex}")
            return None

    async def fetch_coinbase_spot_price(self, symbol):
        try:
            ticker = await self.coinbasepro.fetch_ticker(symbol)
            self.logger.debug(f"Fetched Coinbase spot price for {symbol}: {ticker['last']}")
            return ticker['last']
        except Exception as ex:
            self.logger.error(f"Error fetching Coinbase spot price for {symbol}: {ex}")
            return None