# HyperBacktest
An incredibly overengineered (and blazingly fast!) backtesting framework with a neat UI.
## Screenshots:
![Screenshot 2](/screenshots/screen2.png)
![Screenshot 1](/screenshots/screen1.png)
## Run:
### First, you gotta create a virtual environment and install the dependencies
`python3 -m venv .venv && source ./.venv/bin/activate && pip3 install -r requirements.txt`
### Then, you can run the UI on the port you desire:
`python3 app.py 8080`
### You can also run the automatic backtester in the background:
`python3 det_analyzer.py`
Currently it is focused on fetching past trades of hyperliquid wallets to determine copying which wallets could feasibly be profitable if done on your exchange(by default it uses Bybit, but this can be modified). The top wallets' data with associated original ratios is available in `hl_trader_ratios.csv`, for which the code for calculation will be released a bit later.
## How it works:
#### First, `openbooka.py` can fetch past trades of:
* Bybit copy traders
* Binance copy traders
* OKX traders and copy traders
* Hyperliquid wallets
* Discord (FreqAI models' signals in their Discord server currently, but can probably be tweaked to be used for other servers - in any case, you will have to fill your own authorization key)
* Feather file (a table with columns open_time, close_time, ticker, leverage, margin)
Then, it uses the functionality in `downloader.py` to efficiently download past data by filling the gaps in data required for a full backtest. The default timeframe for backtests is `1m`. This high precision is crucial if highly leveraged trades are backtested, so better safe than sorry.

Replace the FLARESOLVERR_PATH variable with the path of your flaresolverr instance at the very top of `openbooka.py`. Take note that you will also need to replace headers/cookies for some of the websites in the code. 

#### Afterwards, a vectorized (and thus blazingly fast) backtester based on polars framework in `backtester.py` is used to backtest the traders on the desired timeframe. The vectorized architecture allows it to do backtests involving millions of datapoints in mere minutes, whereas polars allows for using power of more than one core to speed it up further. After the backtest is done, it calculates a variety of different ratios:
* Sharpe
* Calmar
* Drawdown Capture Ratio
* Information Ratio
* Gittins Index
* Calmar Divergence (Calmar of the strategy - Calmar of the benchmark)

Based on these ratios it calculates some overall metrics:
* Performance
* Rating (based on both Performance and OfficialR, which is the original metric provided from the `hl_trader_ratios.csv` file, with the latter having much smaller weight than the former)

#### After all above is done, it saves the trader data to `./my_data/data/caches/caches.csv`, which allows the results to be viewed within the UI.

P.S. This code is an adaptation from a larger project, so some features are missing. This is not to be used for production purposes(this part of the project has a much higher focus on development speed than code readability), so it is rather to be used just as an inspiration for some design choices, perhaps when it comes to backtest simulation or data downloading(the script here was also adopted for my EE).


