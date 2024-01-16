import ccxt
import pandas as pd
import asyncio
import ccxt.async_support as async_ccxt
import csv

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
        csv_file_path = 'satoshi_rates.csv'  # Define the CSV file path

        while True:  # Loop indefinitely
            self.logger.info("Starting monitoring of Satoshi rates.")
            common_symbols = await self.get_common_symbols()

            if not common_symbols:
                self.logger.info("No common symbols found between exchanges.")
            else:
                current_satoshi_rates = await self.calculate_satoshi_rates_for_symbols(common_symbols)
                
                # Append new Satoshi rates to the CSV file
                with open(csv_file_path, 'a', newline='') as file:
                    writer = csv.writer(file)
                    for symbol, rate in current_satoshi_rates.items():
                        writer.writerow([symbol, rate])

                # Process rate changes and message queuing
                for symbol in current_satoshi_rates:
                    previous_rate = self.previous_satoshi_rates.get(symbol)
                    if previous_rate is not None and previous_rate != 0:
                        rate_change = abs(current_satoshi_rates[symbol] - previous_rate) / previous_rate
                        if rate_change > 0.0001:  # Check the threshold for notifying rate change
                            message = f"SatoshiRate for {symbol} changed by more than 0.02% in the last hour: {current_satoshi_rates[symbol]} compared to {previous_rate}"
                            self.messages_to_send.append(message)
                            self.logger.info(message)
                    elif previous_rate == 0:
                        self.logger.debug(f"Previous rate for {symbol} is zero, skipping rate change calculation.")

                    self.previous_satoshi_rates[symbol] = current_satoshi_rates[symbol]

            self.last_updated_time = datetime.utcnow()
            await asyncio.sleep(60)  # Wait for 60 seconds before next cycle
            await asyncio.sleep(60)  # Wait for 60
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


    def map_bybit_to_coinbase(self, symbol):
        # Remove the additional segment and replace '/USDT' with '/USD'
        symbol = symbol.split(':')[0]
        if symbol.endswith('/USDT'):
            return symbol.replace('/USDT', '/USD')
        return symbol

    def map_coinbase_to_bybit(self, symbol):
        if symbol.endswith('/USD'):
            return symbol.replace('/USD', '/USDT')
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

            # New implementation for symbol mapping
            mapped_symbols_bybit = {s: self.map_bybit_to_coinbase(s) for s in bybit_tickers.keys()}
            symbols_coinbase = {s.split('/')[0] for s in coinbasepro_tickers.keys()}  # Extract base currency for Coinbase symbols

            common_symbols = {s for s in mapped_symbols_bybit.values() if s.split('/')[0] in symbols_coinbase}

            return list(common_symbols)
        except Exception as e:
            self.logger.error(f"Error getting common symbols: {e}")
            return []
    async def calculate_satoshi_rate(self, symbol):
        
        try:
            bybit_symbol = self.map_coinbase_to_bybit(symbol)
            coinbase_symbol = symbol

            bybit_futures_price = await self.fetch_bybit_futures_price(bybit_symbol)
            coinbase_spot_price = await self.fetch_coinbase_spot_price(coinbase_symbol)
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