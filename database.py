import time
import pandas as pd
import ccxt
from os import listdir


def get_new_coins():
    exchange = ccxt.coinbaseexchange()
    response = exchange.fetch_markets()
    markets = []

    for i in range(len(response)):
        market = response[i]['symbol']
        if market.split("/")[1] == "USD":
            markets.append(market)

    coins = list(map(lambda coin: coin.split("/")[0], markets))
    current_coins = list(map(lambda coin: coin.split("_")[0], listdir("pairs/usd_pairs")))
    new_coins = list(set(coins) - set(current_coins))

    if len(new_coins) != 0:
        for coin in new_coins:
            print(coin)
            new_coin = fetch_all_quotes(f"{coin}/USD")
            new_coin.to_csv(f"pairs/usd_pairs/{coin}_USD.csv", index=False)


def find_first_date(pair, initial_date="2013-01-01 00:00:00"):
    exchange = ccxt.coinbaseexchange()
    new_date = exchange.parse8601(initial_date)
    slide = initial_date
    current_time = int(str(time.time()).split(".")[0]) * 1000

    while True:
        miss_size = 300 - len(exchange.fetch_ohlcv(pair, "1d", since=new_date))

        if miss_size == 0:
            hour_miss = 300 - len(exchange.fetch_ohlcv(pair, "1h", since=new_date))
            new_date += hour_miss * 60 * 60 * 1000
            return exchange.iso8601(new_date).replace("T", " ").replace("Z", "")[:-4]

        date_24 = pd.date_range(f"{slide}", periods=miss_size + 1)[-1]
        slide = date_24
        new_date = exchange.parse8601(f"{date_24}")

        if current_time < new_date:
            new_date = exchange.fetch_ohlcv(pair, "1d")[0][0]
            return exchange.iso8601(new_date).replace("T", " ").replace("Z", "")[:-4]


def fetch_all_quotes(pair):
    exchange = ccxt.coinbaseexchange()
    new_date = exchange.parse8601(find_first_date(pair))
    all_quotes = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    while True:
        batch = exchange.fetch_ohlcv(pair, "1h", since=new_date)
        appended_frame = pd.DataFrame()

        if len(batch) == 0:
            print(f"pair {pair} fetched successfully. Pair was delisted, last trading day {new_date}.")
            return all_quotes

        appended_frame[["timestamp", "open", "high", "low", "close", "volume"]] = batch
        all_quotes = pd.concat([all_quotes, appended_frame])
        last_date_in_batch = batch[-1][0]
        new_date = last_date_in_batch + 3600000

        if str(new_date)[:-3] > str(time.time())[:-8]:
            print(f"New pair {pair} fetched successfully.")
            return all_quotes


def update_database():
    exchange = ccxt.coinbaseexchange()
    current_coins = list(map(lambda coin: coin.split("_")[0], listdir("pairs/usd_pairs")))

    print("Database is being updated.")
    for coin in current_coins:
        print(coin)
        pair = pd.read_csv(f"pairs/usd_pairs/{coin}_USD.csv")
        last_entry_timestamp = int(list(pair["timestamp"])[-1])
        current_time = int(str(time.time()).split(".")[0]) * 1000
        relevance_check = (current_time - last_entry_timestamp) < 3600000

        if relevance_check:
            continue

        new_entries_frame = pd.DataFrame()
        try:
            new_entries = exchange.fetch_ohlcv(f"{coin}/USD", "1h", since=last_entry_timestamp + 3600000)
        except:
            continue

        if len(new_entries) == 0:
            continue

        new_entries_frame[["timestamp", "open", "high", "low", "close", "volume"]] = new_entries
        pair = pd.concat([pair, new_entries_frame])
        pair.to_csv(f"pairs/usd_pairs/{coin}_USD.csv", index=False)

    print("Database is up to date.")


if __name__ == "__main__":
    update_database()
