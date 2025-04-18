# Python_project
Our project aims to streamline this process by providing a centralized solution that simplifies data analysis and enhances decision-making for our target audience.
***
***Code review***\
**Database.py** automatically creates and updates the database of all coins from the Coinbase website as needed. This allows for flexible tracking of the most influential and relevant coins. Despite the smaller number of coins, the exchange is the largest by trading volume in the U.S.\
The functions in the database.py file are structured very simply. 
1. get_new_coins() is used only once a month to add new coins that have been listed on the exchange.
2. find_first_date() allows you to find the first date and hour when a coin started trading in case we are creating the database for the first time. Additionally, this function identifies situations when a coin has been delisted from the exchange and notifies the user about it.
3. fetch_all_quotes() takes this first date and downloads all quotes for the coin from its inception to the present (the quotes are hourly). Once the database is created, these functions become less important, and the remaining work is handled by update_database(), which finds the last saved date (to the hour) in the database and downloads new hourly values, updating the .csv file.\
The key library in this work is *ccxt*, which allows for free requests for quotes from various exchanges. It also standardizes the APIs of all exchanges and creates a unified, user-friendly interface for simple commands.


The file **spreads_calculator.py** creates several tables. The first one, *close_price_hist.csv*, saves the latest closing prices of coins from a specified list for all selected exchanges. The files *spreads_abs.csv* and *spreads_pct.csv* store the spreads between all exchanges across all coins. The only difference between them is in their meaning—spreads_abs stores the differences in absolute terms, while spreads_pct stores them in percentage terms.
The exchanges and coins were selected based on liquidity criteria, which is why major exchanges such as Bybit, Binance, Crypto.com, and others are represented here. As for the coins, BTC, ETH, DOGE, SOL, and others were chosen, of course.
The code calculates spreads and sends messages to a Telegram bot on your channel. The code itself consists of several functions. 
1. parse_current_quotes() simply gathers the most current spot prices for the coins and saves them.
2. get_spreads() calculates the spreads between these quotes using custom functions custom_diff() and custom_pct_change().
3. get_best_spreads() takes the output of the get_spreads() function from the spreads_pct.csv file and selects the maximum value, forming the best spread for each coin.
4. get_bot_message() generates the bot message, which includes information about the coin, potential profit in percentage terms, and the exchanges where one can first buy the coin with USDT and then sell it to realize that profit.\
The remaining functions are utility functions necessary only for interacting with the Telegram API.
