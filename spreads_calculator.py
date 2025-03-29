import time
import pandas as pd
import ccxt
import telebot as tb


engines = {"binance": ccxt.binance(), "bybit": ccxt.bybit(), "okx": ccxt.okx(), "kucoin": ccxt.kucoin(),
           "bitrue": ccxt.bitrue(), "gateio": ccxt.gate(), "htx": ccxt.huobi(), "cryptocom": ccxt.cryptocom(),
           "mexc": ccxt.mexc(), "bitget": ccxt.bitget()}

coins = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "DOGE/USDT", "BONK/USDT", "LINK/USDT", "WIF/USDT", "PEPE/USDT", "XRP/USDT",
         "BCH/USDT", "NEAR/USDT", "AVAX/USDT"]

coins_sorted = ["AVAX/USDT", "BCH/USDT", "BONK/USDT", "BTC/USDT", "DOGE/USDT", "ETH/USDT", "LINK/USDT",
                "NEAR/USDT", "PEPE/USDT", "SOL/USDT", "WIF/USDT", "XRP/USDT"]


def parse_current_quotes():
    time_start = time.time()
    call_result = pd.DataFrame(columns=["exchange", "pair", "timestamp", "lastClose"])

    for name, api in engines.items():
        to_frame = pd.DataFrame(columns=["exchange", "pair", "timestamp", "lastClose"])
        exchange = api
        if name == "cryptocom":

            for coin in coins:
                try:
                    res = exchange.fetch_ticker(coin)
                    info = [name, res['symbol'], res['timestamp'], res['close']]
                    to_frame.loc[len(to_frame)] = info
                except:
                    log = open("coin_error.txt", "a")
                    log.write(f"Caught exception on {coin} at {time_start} time. "
                              f"Check {name} exchange on currency presense.")
            call_result = pd.concat([call_result, to_frame])
            continue

        api_call = list(exchange.fetch_tickers(coins).values())

        for coin in api_call:
            info = [name, coin['symbol'], coin['timestamp'], coin['close']]
            to_frame.loc[len(to_frame)] = info

        call_result = pd.concat([call_result, to_frame])

    time_end = time.time()
    time_for_done = time_end - time_start
    return call_result, time_for_done, time_start


def custom_diff(list1, list2):
    i = 0
    res = []
    for value in list1:
        diff = float(list1[i]) - float(list2[i])
        res.append(diff)
        i += 1
    return res


def custom_pct_change(diff, initial):
    i = 0
    res = []
    for value in diff:
        change = diff[i]/initial[i]
        res.append(change)
        i += 1
    return res


def get_spreads(fetch_res, probe_time):
    frame_for_spread = fetch_res
    exchanges = list(engines.keys())
    columns = coins
    columns.append("ex_diff")
    spreads_for_ex = pd.DataFrame(columns=columns)
    spreads_pct_for_ex = pd.DataFrame(columns=columns)

    for exchange in list(engines.keys()):
        for compare_exchange in exchanges:
            main_ex = frame_for_spread[frame_for_spread["exchange"] == exchange].sort_values(by=['pair'])['lastClose']
            comp_ex = frame_for_spread[frame_for_spread["exchange"] == compare_exchange].sort_values(by=['pair'])[
                'lastClose']

            spread_list = custom_diff(main_ex.values, comp_ex.values)
            spread_list_pct = custom_pct_change(spread_list, main_ex.values)
            spread_list.append(f"{exchange}-{compare_exchange}")
            spread_list_pct.append(f"{exchange}-{compare_exchange}, %")
            spreads_for_ex.loc[len(spreads_for_ex)] = spread_list
            spreads_pct_for_ex.loc[len(spreads_pct_for_ex)] = spread_list_pct

    spreads_for_ex.columns = ["AVAX/USDT", "BCH/USDT", "BONK/USDT", "BTC/USDT", "DOGE/USDT", "ETH/USDT",
                              "LINK/USDT", "NEAR/USDT", "PEPE/USDT", "SOL/USDT", "WIF/USDT", "XRP/USDT", "ex_diff"]
    spreads_pct_for_ex.columns = ["AVAX/USDT", "BCH/USDT", "BONK/USDT", "BTC/USDT", "DOGE/USDT", "ETH/USDT",
                                  "LINK/USDT", "NEAR/USDT", "PEPE/USDT", "SOL/USDT", "WIF/USDT", "XRP/USDT", "ex_diff"]

    database_abs = pd.read_csv("pairs/best_spreads/spreads_abs.csv")
    database_pct = pd.read_csv("pairs/best_spreads/spreads_pct.csv")
    database_close = pd.read_csv("pairs/best_spreads/close_price_hist.csv")
    last_id = database_abs["probe_id"].max()

    spreads_for_ex["probe_id"] = last_id + 1
    spreads_for_ex["probe_time"] = probe_time
    spreads_pct_for_ex["probe_id"] = last_id + 1
    spreads_pct_for_ex["probe_time"] = probe_time
    fetch_res["probe_id"] = last_id + 1
    fetch_res["probe_time"] = probe_time

    database_abs = pd.concat([database_abs, spreads_for_ex])
    database_pct = pd.concat([database_pct, spreads_pct_for_ex])
    database_close = pd.concat([database_close, fetch_res])

    database_abs.to_csv("pairs/best_spreads/spreads_abs.csv", index=False)
    database_pct.to_csv("pairs/best_spreads/spreads_pct.csv", index=False)
    database_close.to_csv("pairs/best_spreads/close_price_hist.csv", index=False)

    return spreads_for_ex, spreads_pct_for_ex


def get_best_spreads(frame):
    pairs = []
    coins = ["AVAX/USDT", "BCH/USDT", "BONK/USDT", "BTC/USDT", "DOGE/USDT", "ETH/USDT", "LINK/USDT",
             "NEAR/USDT", "PEPE/USDT", "SOL/USDT", "WIF/USDT", "XRP/USDT"]

    for coin in coins:
        biggest_spread = frame.max()[f"{coin}"]
        exchange_pair = frame.iloc[frame[f'{coin}'].astype(float).idxmax()]["ex_diff"]
        pairs.append([biggest_spread, coin, exchange_pair])
    return pairs


def get_bot_message(pairs):
    mes = "Лучшие спреды по анализируемым монетам:\n"
    for coin in pairs:
        mes += f"{coin[1]} - {round(float(coin[0])*100, 3)}% - Биржи {coin[2][:-3]}\n"
    return mes


def full_cycle():
    new_fetch, ttf, probe_time = parse_current_quotes()
    spread_abs, spread_pct = get_spreads(new_fetch, probe_time)
    pairs = get_best_spreads(spread_pct)
    message = get_bot_message(pairs)
    return message


bot = tb.TeleBot('7467516243:AAHDmbxR-UObsONol-6mNfIKlwtZla1pLrM')


@bot.message_handler(commands=['/start'])
def start_message(message):
    bot.send_message(message.chat.id, message.chat.id)
    bot.send_message(message.chat.id, full_cycle())


def send_exchange(message):
    bot.send_message(-4255136705, message)


try:
    send_exchange(full_cycle())
except:
    try:
        send_exchange(full_cycle())
    except:
        send_exchange("Ошибка, проверьте сгенерированный лог.")







