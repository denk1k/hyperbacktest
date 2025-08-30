import asyncio
import json
import logging
import time
import traceback
import nest_asyncio
from tqdm import tqdm

import polars as pl
import numpy as np
from typing import Optional
import math

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
from downloader import download_past_data

from datetime import timedelta, datetime, timezone
from typing import Optional, List, Dict, Any, Tuple
import plotly.graph_objects as go
import numpy as np
from plotly.subplots import make_subplots
import pandas as pd
import polars as pl
import math
from historical_binance import BinanceDataProvider
import os
import httpx
exchange = "bybit"





























def create_folder_if_does_not_exist(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print("Path folder created", path)
    return path


REQUIRED_TICKERS = ["BTC/USDT:USDT", "SOL/USDT:USDT", "ETH/USDT:USDT"]
DEFAULT_BENCHMARK = {i: 1 / len(REQUIRED_TICKERS) for i in REQUIRED_TICKERS}

async def get_ticker_data():
    ses = httpx.AsyncClient()
    resp = await ses.get("https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000")
    a = resp.json()
    res = {}
    for item in a["result"]["list"]:
        if item["symbol"].endswith("USDT"):
            symbol = item["symbol"].replace("USDT", "/USDT:USDT")
            res[symbol] = float(item["lotSizeFilter"]["minOrderQty"])
    return res

def convert_tf(timeframe: str) -> int:
    """
    Converts a timeframe string to minutes.
    :param timeframe: Timeframe string (e.g., '1m', '5m', '1h', '1d').
    :return: Timeframe in minutes.
    """
    if timeframe.endswith("m"):
        return int(timeframe[:-1])
    elif timeframe.endswith("h"):
        return int(timeframe[:-1]) * 60
    elif timeframe.endswith("d"):
        return int(timeframe[:-1]) * 1440
    else:
        raise ValueError(f"Unsupported timeframe format: {timeframe}")

class Backtester:
    dataprovider = None
    backtest_cacher = None
    open_book = None
    limit_to_copytraded = True
    already_downloaded = []
    MAIN_PATH = None
    TICKER_PATH = None
    TICKER_NAME = "{currency}_USDT_USDT-{timeframe}-futures.feather"
    CACHE_PATH = None
    CACHE_NAME = "cache.csv"
    OPENBOOK_CACHE_PATH = None

    def initialize_open_book(self, namespace="orange", semaphore_count=4):
        
        from openbooka import OpenBook
        self.open_book = OpenBook(cache_directory=self.OPENBOOK_CACHE_PATH, namespace=namespace, semaphore_compiler=semaphore_count, process_subname=None)

    async def okx_get_days_multiplier(self, combined_df: pl.DataFrame, Trader_Address, total_balance_name):
        
        
        
        
        days_true_count, ratio_graph = await self.open_book.okx_fetch_extended_day_count(Trader_Address)
        dfr = combined_df.select(["date", total_balance_name]).with_columns(pl.col("date").dt.date().alias("day"))

        daily_balance = dfr.group_by("day").agg(pl.col(total_balance_name).max().alias("daily_total_balance"))

        daily_balance = daily_balance.with_columns(
            (pl.col("daily_total_balance").ne(pl.col("daily_total_balance").shift(1))).alias("balance_changed")
        )

        days_with_changes = daily_balance.filter(pl.col("balance_changed")).height - 1
        days_multiplier = days_true_count / days_with_changes
        return days_multiplier, ratio_graph, days_with_changes

    async def receive_trades(self, trader_info: [dict[str, str]], reverse):
        if self.limit_to_copytraded:
            self.open_book.limit_to_copytraded()
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        print(self.open_book.pairlist)
        sources = pd.DataFrame(trader_info)  
        sources["uid"] = sources["Trader Address"]
        self.open_book.set_sources(sources)
        trades = await self.open_book.live_compile_all(False, False, 10000000, False, reverse)
        logger.warning(f"Detected {trades.shape[0]} trades")
        trades = trades.sort_values(by="open_time")
        print(trades.to_markdown())
        plt = pl.from_pandas(trades.reset_index())
        return plt

    def reset_downloaded_queue(self):
        self.already_downloaded = []

    def calculate_leverage_atr(self, leverage_setup, combined_df, close_col_name, open_time):
        length = leverage_setup["atr_length"]
        minutes = leverage_setup["atr_minutes"]
        stt = open_time - timedelta(
            minutes=length * minutes) 
        test_df = combined_df.filter(pl.col("date").is_between(stt, open_time)).clone().select(
            ["date", close_col_name]).sort("date").select(pl.all().forward_fill())
        test_df = (
            test_df
            .group_by_dynamic("date", every=f"{minutes}m", closed="right")
            .agg([
                pl.col(close_col_name).max().alias("high"),
                pl.col(close_col_name).min().alias("low"),
                pl.col(close_col_name).last().alias("close")
            ])
        ).sort("date").select(pl.all().forward_fill())

        test_df = test_df.with_columns((pl.col("high") - pl.col("low"))
                                       .abs()
                                       .rolling_mean(test_df.height)
                                       .alias("atr"))
        max_date = test_df["date"].max()
        final_atr = test_df.filter(pl.col("date") == max_date)["atr"].item(0)
        close_value = test_df.filter(pl.col("date") == max_date)["close"].item(0)
        if final_atr is None or final_atr == 0 or close_value is None:
            logger.error(f"Error in final-atr: {test_df}")
            return 1
        atr_leverage = close_value / final_atr * leverage_setup["leverage"]
        print(final_atr, "is the final atr", atr_leverage, "is leverage")
        return max(atr_leverage, 1)

    async def backtest_traders(self, trader_info: [dict[str, str]], reverse=False, multi_directional=False,
                               leverage_setup: dict = None, isolated=False, quantile_setup = None, margin_scale=True):
        print(trader_info, "is the lokin")
        try:
            if ('rating' in list(trader_info[0].keys()) or len(trader_info) > 1) or margin_scale:
                advanced_backtest = True
                multi_directional = False  
                logger.info("Running advanced backtest.")

            else:
                advanced_backtest = False

            is_default = (leverage_setup is None or (
                    leverage_setup["leverage"] == 1 and leverage_setup["type"] == "static")) and (
                                 reverse is False) and (multi_directional is False) and (len(trader_info) == 1)
            if leverage_setup is None:
                leverage_setup = {"leverage": 1, "type": "static"}

            if quantile_setup is None:
                quantile_setup = {"yolomax": .1, "yolometer": .1}
            default_timeframe = "1m"
            modes = ["normal"]
            trades = await self.receive_trades(trader_info, reverse)
            print(trades)

            current_date = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=5)
            logger.info(f"Current date: {current_date}")
            trades = trades.with_columns(
                pl.col("close_time").fill_null(
                    current_date
                ),
                
            )
            min_start_date = trades["open_time"].min()
            if leverage_setup["type"] == "atr":
                min_start_date = min_start_date - timedelta(
                    minutes=leverage_setup["atr_length"] * leverage_setup["atr_minutes"])
            max_start_date = trades["open_time"].max()
            logger.info(f"Initial pairlist: {trades['ticker'].unique().to_list()}, Trade count: {trades.height}")

            if self.limit_to_copytraded:
                trades = trades.filter(pl.col("ticker").is_in(self.open_book.pairlist))
                pairlist = list(set(REQUIRED_TICKERS + trades["ticker"].unique().to_list()))
                logger.info(f"Limiting trades to copytraded: {pairlist}, Trade count: {trades.height}")
            else:
                pairlist = list(set(REQUIRED_TICKERS + trades["ticker"].unique().to_list()))

            day_count_to_d = max((pd.Timestamp.utcnow()-min_start_date).days+1,20)
            pairlist_to_download = [pair for pair in pairlist if pair not in self.already_downloaded]


            if self.dataprovider is None:

                

                download_past_data(pairlist_to_download, exchange, convert_tf(default_timeframe), day_count=day_count_to_d)
                self.already_downloaded.extend(pairlist_to_download)

                self.dataprovider = BinanceDataProvider(pairlist, [default_timeframe], self.TICKER_PATH, self.TICKER_NAME)
                await self.dataprovider.load_tickers()
                
            else:

                
                
                
                

                download_past_data(pairlist_to_download, exchange, convert_tf(default_timeframe), day_count=day_count_to_d)
                self.already_downloaded.extend(pairlist_to_download)
                self.dataprovider = BinanceDataProvider(pairlist, [default_timeframe], self.TICKER_PATH, self.TICKER_NAME)
                await self.dataprovider.load_tickers()

            combined_df = None
            

            
            for pair in pairlist:
                symbol = pair.split("/")[0]
                if self.dataprovider.cached_dataframes[default_timeframe][pair] is None:
                    print(f"[red]DF does not exist for {pair}")

                    trades = trades.filter(pl.col("ticker") != pair)
                    continue
                
                df = self.dataprovider.cached_dataframes[default_timeframe][pair].clone().drop(
                    ["open", "high", "low", "volume"]).filter(
                    pl.col("date") >= min_start_date - timedelta(minutes=10)).select(
                    pl.all().forward_fill().fill_null(strategy="forward").fill_null(strategy="backward"))
                print(df.height, self.dataprovider.cached_dataframes[default_timeframe][pair].clone())
                for mode in modes:
                    df = df.with_columns(
                        (pl.lit(0).alias(f"short_position_count_{mode}"), pl.lit(0).alias(f"long_position_count_{mode}"),
                         pl.lit(0).alias(f"locked_balance_{mode}"), pl.lit(0).alias(f"unrealized_balance_{mode}")))
                
                df_min_date, df_max_date = df["date"].min(), df["date"].max()
                if df.height < 1:
                    print("[yellow]Insufficient data before min date for", symbol)
                    
                    trades = trades.filter(pl.col("ticker") != pair)
                    continue
                
                
                df = (df.rename({col: f"{symbol}_{col}" for col in df.columns if col != "date"}))
                if combined_df is None:
                    combined_df = df
                else:
                    combined_df = combined_df.join(df, on="date", how="outer_coalesce").unique(subset=["date"]).sort(
                        "date").select(
                        pl.all().forward_fill().fill_null(strategy="forward").fill_null(
                            strategy="backward"))  
            pairlist = trades["ticker"].unique().to_list()
            trades = trades.filter(
                (pl.col("open_time") > combined_df["date"].min()) & (pl.col("close_time") < combined_df["date"].max()) & (
                        pl.col("open_time") < combined_df["date"].max()))
            print("Final pairlist: ", pairlist, " Trade count: ", trades.height)
            
            results = {}
            ratios = {}
            benchmark_ratios = None

            days_multiplier = None
            ratios_official = None

            is_okx = trader_info[0]["type"] in ("okx", "okxacc") and len(trader_info) == 1

            for mode in modes:
                drawdowns = []
                normal_mode = mode == "normal"
                ub_name = "unlocked_balance_" + mode
                tt_count_name = "total_trade_count_" + mode
                tt_pos_count_name = f"total_position_count_{mode}"
                this_trade_profit_name = f"this-trade-profit_{mode}"
                initial_investment = 1000000
                infinite_margin_investment = initial_investment
                commission_fee = 0.0008
                if mode == "long_only":
                    trades_current = trades.clone().filter(pl.col("is_short") == False)
                elif mode == "short_only":
                    trades_current = trades.clone().filter(pl.col("is_short") == True)
                else:
                    trades_current = trades.clone()

                yolometer = None
                position_values = {}

                
                combined_df = combined_df.with_columns(pl.lit(initial_investment).alias(ub_name),
                                                       pl.lit(0).alias(tt_count_name))
                combined_df = combined_df.with_columns(pl.all().forward_fill().fill_null(strategy="forward"))
                if advanced_backtest:
                    tinfo_df = pl.from_dicts(trader_info)
                    print("Doing analysis for advanced backtest")
                    combined_df = combined_df.with_columns(pl.lit(0).alias("yolometer"))
                    for trader_tagname in trades_current["owner"].unique().to_list():
                        tyoloname = f"abs_pos_{trader_tagname}"
                        combined_df = combined_df.with_columns(pl.lit(0).alias(tyoloname))
                        trades_for_trader = trades_current.clone().filter(pl.col("owner").eq(trader_tagname))
                        combined_df = combined_df.with_columns(pl.lit(0).alias(f"yolo_{trader_tagname}"))

                        for row in trades_for_trader.iter_rows(named=True):
                            print(row['margin'])
                            symbol = row["ticker"].split("/")[0]
                            short_pos_col = f"{symbol}_short_position_count_{mode}_{trader_tagname}"
                            long_pos_col = f"{symbol}_long_position_count_{mode}_{trader_tagname}"
                            if long_pos_col not in combined_df.columns:
                                combined_df = combined_df.with_columns(pl.lit(0).alias(long_pos_col))
                            if short_pos_col not in combined_df.columns:
                                combined_df = combined_df.with_columns(pl.lit(0).alias(short_pos_col))
                            print(short_pos_col, long_pos_col)
                            print(row['is_short'])
                            if row["is_short"]:

                                
                                combined_df = combined_df.with_columns((
                                    pl.when(
                                        (pl.col("date") >= row["open_time"]) & (pl.col("date") <= row["close_time"])
                                    ).then(pl.col(short_pos_col) + 1 if not margin_scale else row["margin"] * row["leverage"]).otherwise(pl.col(short_pos_col)).alias(short_pos_col)
                                ))
                            else:


                                combined_df = combined_df.with_columns((
                                    pl.when(
                                        (pl.col("date").is_between(row["open_time"], row["close_time"]))
                                    ).then(pl.col(long_pos_col) + 1 if not margin_scale else row["margin"]  * row["leverage"]).otherwise(pl.col(long_pos_col)).alias(long_pos_col)
                                ))
                        for ticker in trades_for_trader["ticker"].unique().to_list():
                            symbol = ticker.split("/")[0]
                            short_pos_col = f"{symbol}_short_position_count_{mode}_{trader_tagname}"
                            long_pos_col = f"{symbol}_long_position_count_{mode}_{trader_tagname}"
                            combined_df = combined_df.with_columns(
                                pl.col(tyoloname).add(pl.col(long_pos_col).sub(pl.col(short_pos_col)).abs()).alias(
                                    tyoloname),
                                pl.col(long_pos_col).sub(pl.col(short_pos_col)).alias(f"{symbol}_yolo_{trader_tagname}"))
                        

                        yolomax = combined_df.select(tyoloname).max().item()
                        if tinfo_df.height > 1:
                            relative_elo = tinfo_df.filter(pl.col("tagname").eq(trader_tagname)).select("rating").item() / tinfo_df.select(pl.sum("rating")).item()
                        else:
                            relative_elo = yolomax
                        logger.info(f"Relative ELO of {trader_tagname} is {relative_elo}")
                        logger.info(f"Assigned yolo for {trader_tagname} is {yolomax}")
                        
                        print("RAWRRRRR")
                        if yolomax == 0:
                            multiplier = 1
                        else:
                            multiplier = relative_elo / yolomax
                        logger.info(f"Multiplier for {trader_tagname} is {multiplier}")
                        position_values[trader_tagname] = multiplier
                    
                    
                    
                    for trader_tagname in trades_current["owner"].unique().to_list():
                        trades_for_trader = trades_current.clone().filter(pl.col("owner").eq(trader_tagname))
                        multiplier = position_values[trader_tagname]

                        for ticker in trades_for_trader["ticker"].unique().to_list():
                            symbol = ticker.split("/")[0]
                            short_pos_col = f"{symbol}_short_position_count_{mode}_{trader_tagname}"
                            long_pos_col = f"{symbol}_long_position_count_{mode}_{trader_tagname}"
                            if f"yolo_{symbol}" not in combined_df.columns:
                                combined_df = combined_df.with_columns(pl.lit(0).alias(f"yolo_{symbol}"))
                            
                            
                            essex = (pl.col(long_pos_col).sub(pl.col(short_pos_col))).mul(multiplier)
                            combined_df = combined_df.with_columns(pl.col(f"yolo_{symbol}").add(essex))
                    for ticker in trades_current["ticker"].unique().to_list():
                        symbol = ticker.split("/")[0]
                        long_pos_all_traders = f"{symbol}_long_position_count_{mode}"
                        short_pos_all_traders = f"{symbol}_short_position_count_{mode}"
                        combined_df = combined_df.with_columns(pl.when(pl.col(f"yolo_{symbol}").gt(0)).then(
                            pl.col(long_pos_all_traders).add(pl.col(f"yolo_{symbol}").abs())).otherwise(
                            pl.col(long_pos_all_traders)), pl.when(pl.col(f"yolo_{symbol}").lt(0)).then(
                            pl.col(short_pos_all_traders).add(pl.col(f"yolo_{symbol}").abs())).otherwise(
                            pl.col(short_pos_all_traders)))
                        combined_df = combined_df.with_columns(pl.col(f"yolometer").add(pl.col(f"yolo_{symbol}").abs()))
                    
                    yolometer = combined_df.select("yolometer").max().item()
                    logger.info(f"yolometer is {yolometer}")







                else:
                    print(combined_df)
                    for row in trades_current.rows(named=True):
                        
                        symbol = row["ticker"].split("/")[0]
                        short_pos_col = f"{symbol}_short_position_count_{mode}"
                        long_pos_col = f"{symbol}_long_position_count_{mode}"

                        try:
                            allow_short_entry = combined_df.filter(pl.col("date") == row["open_time"]).select(
                                pl.col(long_pos_col)).item() == 0 or multi_directional
                            allow_long_entry = combined_df.filter(pl.col("date") == row["open_time"]).select(
                                pl.col(short_pos_col)).item() == 0 or multi_directional
                            
                        except Exception as e:
                            logger.error(str(e) + " Error on the open time column " + str(row))
                            continue
                        allowed = ((allow_short_entry and row["is_short"]) or (allow_long_entry and not row["is_short"]))
                        if (not allowed) and (not multi_directional):
                            logger.info(f"Rejected trade for {row}")
                            continue
                        combined_df = combined_df.with_columns(
                            total_trade_count=pl.when(
                                pl.col("date").is_between(row["open_time"], row["close_time"]),
                                pl.col(short_pos_col).eq(0),
                                pl.col(long_pos_col).eq(0)
                            ).then(pl.col(tt_count_name) + 1).otherwise(pl.col(tt_count_name)))
                        if row["is_short"]:
                            print(" short")
                            combined_df = combined_df.with_columns((
                                pl.when(
                                    (pl.col("date") >= row["open_time"]) & (pl.col("date") <= row["close_time"])
                                ).then(pl.col(short_pos_col) + 1).otherwise(pl.col(short_pos_col)).alias(short_pos_col)
                            ))
                        else:
                            combined_df = combined_df.with_columns((
                                pl.when(
                                    (pl.col("date").is_between(row["open_time"], row["close_time"]))
                                ).then(pl.col(long_pos_col) + 1).otherwise(pl.col(long_pos_col)).alias(long_pos_col)
                            ))

                position_count_columns = [f"{symbol.split('/')[0]}_short_position_count_{mode}" for symbol in pairlist] + \
                                         [f"{symbol.split('/')[0]}_long_position_count_{mode}" for symbol in pairlist]

                

                combined_df = combined_df.with_columns(
                    pl.sum_horizontal(position_count_columns).alias(tt_pos_count_name)
                )

                
                max_position_count = combined_df[tt_pos_count_name].max() if not advanced_backtest else yolometer
                days_trade_count_real = combined_df.filter(pl.col(tt_pos_count_name) > 0).height / 1440
                

                logger.info(f"Total days in trades: {days_trade_count_real}")
                max_trade_count = combined_df[tt_count_name].max()
                
                
                
                
                

                print(f"{mode}: Maximum position count at any moment: {max_position_count}")
                print(f"{mode}: Maximum trade count at any moment: {max_trade_count}")

                

                
                trades_current = trades_current.with_columns((pl.col('margin') * pl.col('leverage')).alias('volume'))

                
                short_trades_volume = trades_current.filter(pl.col('is_short') == True)['volume'].sum()
                long_trades_volume = trades_current.filter(pl.col('is_short') == False)['volume'].sum()

                
                proportion = min(short_trades_volume, long_trades_volume) / max(short_trades_volume, long_trades_volume)
                if long_trades_volume > short_trades_volume:
                    proportion *= -1

                for row in trades_current.rows(named=True):
                    print(row['open_time'], row['ticker'])
                    if combined_df.filter(pl.col("date") == row["close_time"]).height == 0:
                        print("Error with trade's close time", row)
                        continue
                    symbol = row["ticker"].split("/")[0]
                    short_pos_col = f"{symbol}_short_position_count_{mode}"
                    long_pos_col = f"{symbol}_long_position_count_{mode}"
                    locked_balance_col = f"{symbol}_locked_balance_{mode}"
                    unrealized_col = f"{symbol}_unrealized_balance_{mode}"
                    close_col = f"{symbol}_close"

                    try:
                        open_candle = combined_df.filter(pl.col("date") == row["open_time"])
                        before_open_candle = combined_df.filter(pl.col("date") == row["open_time"] - timedelta(minutes=1))
                        starting_price = float(open_candle[close_col].item())
                        close_candle = combined_df.filter(pl.col("date") == row["close_time"])
                        filtered_df = combined_df.filter(
                            (pl.col('date') >= pl.lit(row["open_time"])) & (pl.col('date') <= pl.lit(row["close_time"]))
                        )
                        
                        
                        
                        

                        
                        has_nan = filtered_df[close_col].is_null().any()

                        if has_nan:
                            raise Exception(f"There are NaN values in {close_col} between the specified dates.")
                    except Exception as e:
                        
                        print(e)
                        
                        continue
                    position_count_change = 1 if not advanced_backtest else position_values[row["owner"]]
                    if margin_scale:
                        position_count_change = row["margin"] * position_count_change * row["leverage"]
                    if row["is_short"] and open_candle[short_pos_col].item() <= 0:
                        logger.info(f"(Short on {symbol})Trade's existence has been denied by the populator")
                        continue
                    if (not row["is_short"]) and open_candle[long_pos_col].item() <= 0:
                        logger.info(f"(Long on {symbol})Trade's existence has been denied by the populator")
                        continue

                    final_type_col = short_pos_col if row["is_short"] else long_pos_col
                    
                    
                    
                    
                    
                    
                    
                    
                    leverage = 1  
                    if leverage_setup["type"] == "static":
                        leverage = leverage_setup["leverage"]
                    elif leverage_setup["type"] == "atr":
                        leverage = self.calculate_leverage_atr(leverage_setup, combined_df, close_col, row["open_time"])
                    elif leverage_setup["type"] == "diff":
                        if row["ticker"] in leverage_setup["leverage"]:
                            leverage = leverage_setup["leverage"][row["ticker"]]
                        else:
                            leverage = leverage_setup["leverage"]["OTHER"]



                    if advanced_backtest:
                        locked_usdt_balance_columns = [f"{symbolr.split('/')[0]}_locked_balance_{mode}" for symbolr in pairlist]
                        total_balance = open_candle[ub_name].item() + sum([open_candle[colname].item() for colname in locked_usdt_balance_columns])
                        margin_usdt = total_balance * (position_count_change / yolometer)

                        minimum_contract_count = self.ticker_info.get(row["ticker"], 1)
                        contract_price = open_candle[close_col].item()
                        logger.info(f"Minimal contract count: {minimum_contract_count}@{contract_price:.3f}")
                        min_order_value = minimum_contract_count * contract_price / leverage
                        margin_final = (margin_usdt // min_order_value) * min_order_value
                        margin_final_test = (margin_usdt * 1.3 // min_order_value) * min_order_value
                        if margin_final == 0 and margin_final_test > 0:
                            logger.info(f"Registered ability to enter: {margin_final}, moving to {margin_final_test}")
                            margin_usdt = margin_final_test
                        elif margin_final == 0:
                            logger.info(f"Cannot enter trade on {symbol}, {margin_final}")
                            continue
                        else:
                            logger.info(f"Moving margin size from {margin_usdt} to {margin_final}")
                            margin_usdt = margin_final
                        if margin_usdt > open_candle[ub_name].item():
                            logger.info(f"Could not entre trade on {symbol} as {margin_usdt}USDT exceeds {open_candle[ub_name].item()}USDT available balance")
                            continue




                    else:
                        marginal_divider = (
                                    max_position_count + position_count_change - open_candle[tt_pos_count_name].item())
                        if marginal_divider <= 0:
                            logger.info(f"Skipping trade on {symbol} as no place to execute")
                            continue
                        margin_usdt = open_candle[ub_name].item() / marginal_divider * position_count_change

                    

                    
                    change_condition = (((pl.col(close_col) - starting_price) / starting_price * leverage + 1) if not row[
                        "is_short"] else ((starting_price - pl.col(close_col)) / starting_price * leverage + 1)) * (
                                               1 - commission_fee)
                    
                    combined_df = combined_df.with_columns((

                        pl.when((pl.col("date").is_between(row["open_time"], row["close_time"]))).then(
                            margin_usdt + pl.col(locked_balance_col)).otherwise(pl.col(locked_balance_col)).alias(
                            locked_balance_col),
                        pl.when((pl.col("date").is_between(row["open_time"], row["close_time"]))).then(
                            pl.col(ub_name) - margin_usdt).otherwise(pl.col(ub_name)).alias(
                            ub_name),
                        pl.when(
                            (pl.col("date").is_between(row["open_time"], row["close_time"]))
                        ).then(margin_usdt * change_condition).otherwise(0).alias(this_trade_profit_name)
                        
                    ))
                    if True:
                        first_zero_date = combined_df.filter(
                            (pl.col("date").is_between(row["open_time"], row["close_time"])) & (
                                pl.col(this_trade_profit_name).le(0)))["date"].min()
                        
                        minimum_value = (
                            combined_df.filter((pl.col("date").is_between(row["open_time"], row["close_time"])))
                            .select(pl.col(this_trade_profit_name).min())
                            .item()
                        )
                        initial_value = (
                            combined_df.filter((pl.col("date").eq(row["open_time"])))
                            .select(pl.col(this_trade_profit_name))
                            .item()
                        )

                        if initial_value is not None and initial_value != 0:
                            logger.info(f"{symbol} trade's minimum value is {minimum_value}/{initial_value}")
                            drawdown = minimum_value / initial_value - 1
                            logger.info(f"Trade's maximum drawdown: {drawdown * 100:.4f}%")
                            drawdowns.append(drawdown)
                            if isolated and first_zero_date is not None:
                                combined_df = combined_df.with_columns(
                                    pl.when(pl.col('date') >= first_zero_date)
                                    .then(0).otherwise(this_trade_profit_name).alias(this_trade_profit_name)
                                )
                                logger.warning(
                                    f"Liquidation on {symbol} has occurred at {first_zero_date}, {row['close_time']} is the close time")

                    combined_df = combined_df.with_columns(
                        pl.when(
                            (pl.col("date").is_between(row["open_time"], row["close_time"]))
                        ).then(pl.col(this_trade_profit_name) + pl.col(unrealized_col)).otherwise(
                            pl.col(unrealized_col)).alias(
                            unrealized_col),

                    )
                    close_candle = combined_df.filter(pl.col("date") == row["close_time"])
                    
                    
                    final_balance = close_candle[this_trade_profit_name].item(0)
                    
                    unrealized_balance_change = final_balance - margin_usdt
                    roi_this_trade = (unrealized_balance_change / margin_usdt) if (margin_usdt != 0) else 1
                    logger.info(f"Profit is {roi_this_trade*100:.2f}")
                    infinite_margin_investment = infinite_margin_investment * ((final_balance/margin_usdt) if margin_usdt != 0 else 1)
                    logger.info(f"Infinite margin investment is {infinite_margin_investment}")
                    combined_df = combined_df.with_columns(pl.when(
                        (pl.col("date") > row["close_time"])
                    ).then(unrealized_balance_change + pl.col(ub_name)).otherwise(pl.col(ub_name)).alias(
                        ub_name))
                usdt_balances = [f"{symbol.split('/')[0]}_unrealized_balance_{mode}" for symbol in pairlist]
                
                tb_name = f"total_balance_{mode}"
                locked_bal_name = f"locked_balance_{mode}"
                combined_df = combined_df.with_columns(
                    pl.sum_horizontal(usdt_balances + [ub_name]).alias(tb_name),
                    pl.sum_horizontal(usdt_balances).alias(locked_bal_name)
                )
                
                
                
                
                
                
                
                
                
                multiplier = float(combined_df.select(pl.last(tb_name)).item()) / float(combined_df.select(pl.first(tb_name)).item()) - 1
                if (not multiplier) or multiplier < 0:
                    multiplier = 0
                print("Gain is", multiplier)
                days_trade_count = combined_df.select(
                    (pl.col(locked_bal_name).fill_null(0)).sum()
                ).item()


                
                
                
                
                

                

                if normal_mode:
                    combined_df, benchmark_columns = generate_benchmark(combined_df, DEFAULT_BENCHMARK)
                
                
                

                results[mode] = combined_df.select(
                    *([pl.col(tb_name), pl.col("date"), pl.col(ub_name)] + ([pl.col("benchmark")] if normal_mode else [])))
                if len(drawdowns)>0:
                    logger.info(f"Maximum drawdown: {min(drawdowns) * 100:.4f}%")
                if normal_mode:
                    benchmark_ratios = calculate_ratios(results[mode]["benchmark"], timeframe_in_minutes=1,
                                                        days_trade_count=results[mode].height / 1440)

                ratios |= {f"{k}_{mode}": v for k, v in
                           calculate_ratios(combined_df[tb_name], combined_df['benchmark'], timeframe_in_minutes=1,
                                            days_trade_count=days_trade_count).items()}
                ratios['trade_type_proportion_ratio'] = proportion
                print(ratios)

            
            full_df = compose_dataframes(results)
            
            graph, graph_simplified = render_performance_metrics(full_df, ratios, modes, benchmark_ratios,
                                                                 "".join([i["tagname"] for i in trader_info]))
            if is_default:
                graph_simplified, graph_simple_path = self.backtest_cacher.add_backtest(trader_info, full_df, ratios, modes,
                                                                                        graph_simplified)
            else:
                logger.info("Omitting the writing of the values as is not the default type")
                graph_simple_path = ""

            return {"dataframe": full_df, "graph_simple_path": graph_simple_path, "graph": graph,
                    "graph_simple": graph_simplified, "ratios": ratios, "benchmark_ratios": benchmark_ratios,
                    "modes": modes}
        except Exception as e:
            print(traceback.format_exc(), e, "occurred")

    def __init__(self, limit_to_copytraded=False, main_path=None, cache_file_name = "cache.csv", open_book=True, semaphore_count=4, namespace="orange"):
        if main_path is None:
            main_path = "./data"
        self.MAIN_PATH = os.path.realpath(main_path)
        self.CACHE_NAME = cache_file_name
        self.TICKER_PATH = create_folder_if_does_not_exist(os.path.join(main_path, exchange, "futures"))
        self.CACHE_PATH = create_folder_if_does_not_exist(os.path.join(main_path, "caches"))
        self.OPENBOOK_CACHE_PATH = create_folder_if_does_not_exist(os.path.join(self.CACHE_PATH, "openbook"))
        logger.info(f"Main path: {self.MAIN_PATH}")
        logger.info(f"Tickers path: {self.TICKER_PATH}")
        logger.info(f"Cache path: {self.CACHE_PATH}")
        logger.info(f"Openbook cache path: {self.OPENBOOK_CACHE_PATH}")
        self.ticker_info = asyncio.run(get_ticker_data())

        self.backtest_cacher = Cacher(self.CACHE_PATH, self.CACHE_NAME)
        self.limit_to_copytraded = limit_to_copytraded
        if open_book:
            self.initialize_open_book(namespace, semaphore_count)




def gittins_approx(sharpe, horizon=1000, samples=10000):
    if math.isinf(sharpe):
        return sharpe
    if math.isnan(sharpe):
        print("Gittins index calculation failed: Sharpe ratio was NaN. Defaulting to 0.0.")
        return 0.0
    rewards = np.random.normal(loc=sharpe, scale=1.0, size=(samples, horizon))
    
    cum_rewards = np.cumsum(rewards, axis=1)
    gamma_range = np.linspace(sharpe - 3, sharpe + 3, 100)
    
    valid_gammas = []
    for g in gamma_range:
        expected_max_reward = np.mean(np.max(cum_rewards - g * np.arange(1, horizon + 1), axis=1))
        if expected_max_reward >= 0:
            valid_gammas.append(g)
            
    if not valid_gammas:
        return sharpe

    return np.max(valid_gammas)


def calculate_ratios(account_values: pl.Series, benchmark_values: Optional[pl.Series] = None,
                     timeframe_in_minutes=1, days_trade_count=0.0, starting_point=0.0):
    """
    Calculates Sharpe Ratio, Calmar Ratio, Information Ratio, Downside Capture Ratio,
    and Calmar Divergence for a given account series and benchmark series.

    Args:
        account_values (pl.Series): Polars Series of account balance values over time.
        benchmark_values (Optional[pl.Series], optional): Polars Series of benchmark balance
            values over time. Defaults to None.
        timeframe_in_minutes (int, optional): Timeframe interval in minutes. Defaults to 1.
        days_trade_count (float, optional): Number of trading days. Defaults to 0.0.
        starting_point (float, optional): Initial value for ratios. Defaults to 0.0.

    Returns:
        dict: A dictionary containing the calculated ratios: Sharpe Ratio, Calmar Ratio,
            Information Ratio, Downside Capture Ratio, and Calmar Divergence.
    """
    multiplier = timeframe_in_minutes
    multmax = 365 * 24 * (60 / multiplier)
    sharpe_ratio = starting_point
    calmar_ratio = starting_point
    information_ratio = starting_point
    downside_capture_ratio = starting_point
    calmar_divergence = starting_point
    gittins_index = starting_point

    try:
        if account_values is None or len(account_values) < 2:
            ratios = {
                'sharpe_ratio': sharpe_ratio,
                'calmar_ratio': calmar_ratio,
                'information_ratio': information_ratio,
                'downside_capture_ratio': downside_capture_ratio,
                'calmar_divergence': calmar_divergence,
                "days_traded": days_trade_count,
                "gittins_index": gittins_index
            }
            return ratios

        account_series = account_values
        returns = account_series.pct_change().drop_nulls()

        if len(returns) == 0: 
            ratios = {
                'sharpe_ratio': sharpe_ratio,
                'calmar_ratio': calmar_ratio,
                'information_ratio': information_ratio,
                'downside_capture_ratio': downside_capture_ratio,
                'calmar_divergence': calmar_divergence,
                "days_traded": days_trade_count,
                "gittins_index": gittins_index
            }
            return ratios

        max_drawdown_series = (account_series / account_series.cum_max())
        if max_drawdown_series.is_empty() or max_drawdown_series.null_count() == len(max_drawdown_series):
            max_drawdown = 0 
        else:
            min_val = max_drawdown_series.min()
            max_drawdown = min_val - 1 if min_val is not None else 0


        
        log_returns = np.log1p(returns.to_numpy()) 
        if log_returns.size > 0 and not np.all(np.isnan(log_returns)):
            log_mean_return = np.nanmean(log_returns) * np.sqrt(multmax) 
            log_std_return = np.nanstd(log_returns)
            if log_std_return != 0 and not np.isnan(log_std_return) and not np.isnan(log_mean_return):
                sharpe_ratio_temp = log_mean_return / log_std_return
                if not math.isnan(float(sharpe_ratio_temp)):
                    sharpe_ratio = sharpe_ratio_temp
            elif log_std_return == 0 and log_mean_return > 0 : 
                sharpe_ratio = float('inf') 
            elif log_std_return == 0 and log_mean_return < 0:
                sharpe_ratio = float('-inf')
            elif log_std_return == 0 and log_mean_return == 0:
                sharpe_ratio = 0.0


        if abs(max_drawdown) > 1e-9: 
            if len(account_series) >= 2 and account_series.item(0) is not None and account_series.item(-1) is not None and account_series.item(0) != 0:
                calmar_ratio_temp = (account_series.item(-1) / account_series.item(0) - 1) / abs(max_drawdown)
                if not math.isnan(float(calmar_ratio_temp)):
                    calmar_ratio = calmar_ratio_temp
            elif len(account_series) >=2 and abs(max_drawdown) > 1e-9 and account_series.item(0) == 0 and account_series.item(-1) > 0: 
                calmar_ratio = float('inf') 
            elif len(account_series) >=2 and abs(max_drawdown) > 1e-9 and account_series.item(0) == 0 and account_series.item(-1) < 0:
                calmar_ratio = float('-inf')

        if benchmark_values is not None and len(benchmark_values) >= 2:
            benchmark_series = benchmark_values
            benchmark_returns = benchmark_series.pct_change().drop_nulls()

            if len(benchmark_returns) > 0 and len(returns) == len(benchmark_returns): 
                excess_returns = returns - benchmark_returns 

                tracking_error = excess_returns.std() 

                if tracking_error is not None and tracking_error != 0 and not math.isnan(tracking_error):
                    mean_excess_return = excess_returns.mean()
                    if mean_excess_return is not None and not math.isnan(mean_excess_return):
                        information_ratio_temp = (mean_excess_return / tracking_error) * math.sqrt(multmax) 
                        if not math.isnan(float(information_ratio_temp)):
                            information_ratio = information_ratio_temp
                elif tracking_error == 0 and excess_returns.mean() > 0: 
                    information_ratio = float('inf')
                elif tracking_error == 0 and excess_returns.mean() < 0:
                    information_ratio = float('-inf')
                elif tracking_error == 0 and excess_returns.mean() == 0:
                    information_ratio = 0.0


                downside_market_filter = benchmark_returns < 0

                strategy_downside_returns_series = returns.filter(downside_market_filter)
                benchmark_downside_returns_series = benchmark_returns.filter(downside_market_filter)

                if not strategy_downside_returns_series.is_empty() and not benchmark_downside_returns_series.is_empty():
                    strategy_downside_returns_mean = strategy_downside_returns_series.mean()
                    benchmark_downside_returns_mean = benchmark_downside_returns_series.mean()

                    if (strategy_downside_returns_mean is not None and not math.isnan(strategy_downside_returns_mean) and
                            benchmark_downside_returns_mean is not None and not math.isnan(benchmark_downside_returns_mean) and
                            benchmark_downside_returns_mean != 0):

                        downside_capture_ratio_temp = (strategy_downside_returns_mean / benchmark_downside_returns_mean) * 100
                        if not math.isnan(float(downside_capture_ratio_temp)):
                            downside_capture_ratio = downside_capture_ratio_temp

                strategy_calmar = calmar_ratio 

                benchmark_max_drawdown_series = (benchmark_series / benchmark_series.cum_max())
                if benchmark_max_drawdown_series.is_empty() or benchmark_max_drawdown_series.null_count() == len(benchmark_max_drawdown_series):
                    benchmark_max_drawdown = 0
                else:
                    min_bm_val = benchmark_max_drawdown_series.min()
                    benchmark_max_drawdown = min_bm_val - 1 if min_bm_val is not None else 0

                benchmark_calmar = starting_point 
                if abs(benchmark_max_drawdown) > 1e-9:
                    if len(benchmark_series) >= 2 and benchmark_series.item(0) is not None and benchmark_series.item(-1) is not None and benchmark_series.item(0) != 0:
                        benchmark_calmar_ratio_temp = (benchmark_series.item(-1) / benchmark_series.item(0) - 1) / abs(benchmark_max_drawdown)
                        if not math.isnan(float(benchmark_calmar_ratio_temp)):
                            benchmark_calmar = benchmark_calmar_ratio_temp
                    elif len(benchmark_series) >=2 and abs(benchmark_max_drawdown) > 1e-9 and benchmark_series.item(0) == 0 and benchmark_series.item(-1) > 0:
                        benchmark_calmar = float('inf')
                    elif len(benchmark_series) >=2 and abs(benchmark_max_drawdown) > 1e-9 and benchmark_series.item(0) == 0 and benchmark_series.item(-1) < 0:
                        benchmark_calmar = float('-inf')


                if benchmark_calmar != 0 and not math.isinf(benchmark_calmar) and not math.isinf(strategy_calmar): 
                    calmar_divergence_temp = strategy_calmar - benchmark_calmar
                    if not math.isnan(float(calmar_divergence_temp)) and not math.isinf(calmar_divergence_temp):
                        calmar_divergence = calmar_divergence_temp
                elif strategy_calmar == benchmark_calmar: 
                    calmar_divergence = 0.0
                elif math.isinf(strategy_calmar) and benchmark_calmar != 0 and not math.isinf(benchmark_calmar):
                    calmar_divergence = float('inf') if strategy_calmar > 0 else float('-inf') 
                elif math.isinf(benchmark_calmar) and strategy_calmar != 0 and not math.isinf(strategy_calmar):
                    calmar_divergence = -1.0 
        
        gittins_index = gittins_approx(sharpe_ratio)

    except Exception as e:
        pass 
    finally:
        ratios = {
            'sharpe_ratio': sharpe_ratio,
            'calmar_ratio': calmar_ratio,
            'information_ratio': information_ratio,
            'downside_capture_ratio': downside_capture_ratio,
            'calmar_divergence': calmar_divergence,
            "days_traded": days_trade_count,
            "gittins_index": gittins_index
        }
        return ratios



def generate_benchmark(balance_df: pl.DataFrame, settings: dict[str, float], main_column='total_balance_normal'):
    initial_investment = balance_df[main_column].item(0)
    benchmark_columns = []
    for pair, share in settings.items():
        symbol = pair.split("/")[0]
        starting_balance = balance_df[f"{symbol}_close"].item(0)
        invested_fiat = share * initial_investment
        balance_df = balance_df.with_columns(
            ((pl.col(f"{symbol}_close") / starting_balance) * invested_fiat).alias(f'{symbol}_benchmark'))
        benchmark_columns.append(f'{symbol}_benchmark')
    balance_df = balance_df.with_columns(
        benchmark=pl.sum_horizontal(benchmark_columns)
    )

    return balance_df, benchmark_columns


def drop_close_columns(balance_df: pl.DataFrame, pairlist):
    return balance_df.drop([f"{symbol}_close" for symbol in pairlist])


def compose_dataframes(data: {str: pl.DataFrame}):
    combined_df = None
    for key, df in data.items():
        print(df)
        data[key] = df.set_sorted("date").group_by_dynamic("date", every="10m", closed="left").agg(
            [pl.col(col).min().alias(col) for col in df.columns if col != "date"]
        )
        if combined_df is None:
            combined_df = data[key]
        elif combined_df.height >= data[key].height:
            combined_df = combined_df.join(data[key], on="date", how="outer_coalesce")
        elif data[key].height > combined_df.height:
            combined_df = data[key].join(combined_df, on="date", how="outer_coalesce")
        print(f"Composed {key}")
    print(combined_df)
    return combined_df





class Cacher:
    CORE_ID_COLS = ["Trader Address", "type"]
    PRESERVED_STATS_COLS = ["PNL", "PNLMonth", "ACCValue", "MaxACCValue",  "officialR"]
    BACKTEST_PROVIDED_COLS = [
        "sharpe_ratio_normal", "days_traded_normal", "calmar_ratio_normal", "calmar_divergence_normal", "downside_capture_ratio_normal","information_ratio_normal", "trade_type_proportion_ratio", "gittins_index_normal",
        "sharpe_ratio_long_only", "days_traded_long_only", "calmar_ratio_long_only", "gittins_index_long_only",
        "sharpe_ratio_short_only", "days_traded_short_only", "calmar_ratio_short_only", "gittins_index_short_only",
        "file_path", "file_thumb_path", "available", "setup", "modes", "date_updated",
        "tagname" 
    ]
    DERIVED_METRIC_COLS = ["performance", "equilibrium", "rating"]

    DEFAULT_FLOAT_COLS = list(set(
        PRESERVED_STATS_COLS +
        [c for c in BACKTEST_PROVIDED_COLS if 'ratio' in c or 'days' in c or 'index' in c] + 
        DERIVED_METRIC_COLS + ["date_updated"] 
    ))
    DEFAULT_BOOL_COLS = [c for c in BACKTEST_PROVIDED_COLS if c == 'available']
    DEFAULT_STR_COLS = list(set(
        [c for c in BACKTEST_PROVIDED_COLS if 'path' in c or c in ['setup', 'modes', 'tagname']]
    ))

    def __init__(self, cache_path: str, cache_name: str):
        self.cache_dir = cache_path
        self.cache_main_file = os.path.join(self.cache_dir, cache_name)
        self.lock_file = self.cache_main_file + ".lock"
        self.render_dir = os.path.join(self.cache_dir, "renders")

        logger.info(f"Cache directory: {self.cache_dir}")
        logger.info(f"Cache main file: {self.cache_main_file}")
        logger.info(f"Lock file: {self.lock_file}")
        logger.info(f"Render directory: {self.render_dir}")

        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(self.render_dir, exist_ok=True)

        self.cache: Optional[pl.DataFrame] = None
        self._load_cache() 

    

    def _wait_for_lock(self, wait_for_remove: bool = True):
        """Waits until the lock file's status matches wait_for_remove."""
        status_text = "removed" if wait_for_remove else "created"
        waiting = False
        max_wait_time = 30 
        start_time = time.time()
        while wait_for_remove == os.path.exists(self.lock_file):
            if not waiting:
                logger.warning(f"Waiting for lock file '{self.lock_file}' to be {status_text}...")
                waiting = True
            if time.time() - start_time > max_wait_time:
                logger.error(f"Timeout waiting for lock file {self.lock_file} to be {status_text}. Aborting wait.")
                raise TimeoutError(f"Timeout waiting for lock file {self.lock_file}")
            time.sleep(0.1)
        if waiting:
            logger.info(f"Lock file '{self.lock_file}' is {status_text}. Proceeding.")

    def _acquire_lock(self) -> bool:
        """Attempts to acquire the lock."""
        try:
            self._wait_for_lock(wait_for_remove=True)
            with open(self.lock_file, 'x') as f: pass 
            logger.debug(f"Acquired lock: {self.lock_file}")
            return True
        except FileExistsError:
            logger.warning(f"Lock file {self.lock_file} already exists after wait. Retrying lock acquisition.")
            return False
        except Exception as e:
            logger.error(f"Failed to acquire lock {self.lock_file}: {e}", exc_info=True)
            return False

    def _release_lock(self):
        """Releases the lock."""
        if os.path.exists(self.lock_file):
            try: os.remove(self.lock_file); logger.debug(f"Released lock: {self.lock_file}")
            except OSError as e:
                if os.path.exists(self.lock_file): logger.error(f"Failed to remove lock {self.lock_file}: {e}")
                else: logger.debug(f"Lock {self.lock_file} removed by another process.")
        else: logger.debug(f"Lock {self.lock_file} already removed.")

    def _get_default_schema(self) -> Dict[str, pl.DataType]:
        """Returns a dictionary defining the default schema."""
        schema = {}
        schema.update({col: pl.Utf8 for col in self.CORE_ID_COLS})
        schema.update({col: pl.Utf8 for col in self.DEFAULT_STR_COLS})
        schema.update({col: pl.Float64 for col in self.DEFAULT_FLOAT_COLS})
        schema.update({col: pl.Boolean for col in self.DEFAULT_BOOL_COLS})
        
        schema.update({col: pl.Float64 for col in self.DERIVED_METRIC_COLS})
        return schema

    def _get_default_row_dict(self, trader_address: str, type_str: str) -> Dict[str, Any]:
        """Creates a dictionary representing a default row with initial values."""
        schema = self._get_default_schema()
        default_row = {col: None for col in schema} 

        
        default_row.update({col: 0.0 for col in self.DEFAULT_FLOAT_COLS if col != "date_updated"}) 
        default_row.update({col: True for col in self.DEFAULT_BOOL_COLS}) 
        default_row.update({col: "" for col in self.DEFAULT_STR_COLS}) 

        
        default_row["Trader Address"] = trader_address
        default_row["type"] = type_str
        
        default_row["available"] = True 
        default_row["date_updated"] = time.time() 

        
        default_row["PNL"] = 1.0
        default_row["PNLMonth"] = 1.0
        default_row["ACCValue"] = 1.0
        default_row["MaxACCValue"] = 1.0
        default_row["officialR"] = 1.0
        
        default_row["performance"] = 0.0
        default_row["equilibrium"] = 0.0
        default_row["rating"] = 0.0

        return default_row

    def _create_empty_cache(self) -> pl.DataFrame:
        """Creates an empty DataFrame with the default schema."""
        return pl.DataFrame(schema=self._get_default_schema())

    def _ensure_schema(self, df: Optional[pl.DataFrame]) -> pl.DataFrame:
        """Adds missing default columns to a DataFrame."""
        if df is None: return self._create_empty_cache()
        schema = self._get_default_schema()
        missing_cols = {col: dtype for col, dtype in schema.items() if col not in df.columns}
        if missing_cols:
            logger.debug(f"Ensuring schema: adding missing columns {list(missing_cols.keys())}")
            add_exprs = [pl.lit(None).cast(dtype).alias(col) for col, dtype in missing_cols.items()]
            df = df.with_columns(add_exprs)
        return df

    def _load_cache(self):
        """Loads cache from disk, ensuring schema. Called at init."""
        logger.info(f"Attempting initial load from {self.cache_main_file}")
        try:
            if os.path.exists(self.cache_main_file):
                self.cache = pl.read_csv(self.cache_main_file, ignore_errors=True, infer_schema_length=1000)
                if self.cache.height == 0 and os.path.getsize(self.cache_main_file) > 0:
                    self.cache = pl.read_csv(self.cache_main_file, infer_schema_length=1000)
                logger.info(f"Cache loaded from {self.cache_main_file}. Shape: {self.cache.shape}")
                self.cache = self._ensure_schema(self.cache)
            else:
                logger.warning(f"Cache file {self.cache_main_file} does not exist. Initializing empty cache.")
                self.cache = self._create_empty_cache()
        except pl.exceptions.NoDataError:
            logger.warning(f"Cache file {self.cache_main_file} is empty. Initializing empty cache.")
            self.cache = self._create_empty_cache()
        except Exception as e:
            logger.error(f"Error reading cache file {self.cache_main_file} during init: {e}", exc_info=True)
            self.cache = self._create_empty_cache()
        self.cache = self._calculate_performance_metrics_on_df(self.cache)

    def _calculate_performance_metrics_on_df(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculates derived performance metrics on a given DataFrame and returns it."""
        if df is None or df.height == 0:
            return df if df is not None else self._create_empty_cache()
        logger.debug("Calculating performance metrics for the provided DataFrame.")

        
        df = df.with_columns([
            (pl.col("sharpe_ratio_normal").fill_null(0.0).clip(lower_bound=0.0).pow(0.4) *
             pl.col("calmar_ratio_normal").fill_null(0.0).clip(lower_bound=0.0).pow(0.7) *
             pl.col("days_traded_normal").fill_null(0.0).clip(lower_bound=0.0).pow(0.8)).mul(
            (1.0 - (pl.col("downside_capture_ratio_normal").fill_null(100.0) / 100.0)).clip(lower_bound=0.01).pow(2)
        ).mul(
            
            pl.col("information_ratio_normal").fill_null(0.0).clip(lower_bound=0.0).add(pl.lit(1)).sqrt()
        ).mul(
            
            pl.col("PNL").fill_null(0.0).clip(lower_bound=0.0).sqrt()
        ).alias("performance"),
            (pl.col("sharpe_ratio_long_only").fill_null(0.0).clip(lower_bound=0).pow(0.1) *
             pl.col("sharpe_ratio_short_only").fill_null(0.0).clip(lower_bound=0).pow(0.1) *
             pl.col("calmar_ratio_long_only").fill_null(0.0).clip(lower_bound=0).pow(0.15) *
             pl.col("calmar_ratio_short_only").fill_null(0.0).clip(lower_bound=0).pow(0.15) *
             pl.col("days_traded_long_only").fill_null(0.0).pow(0.2) *
             pl.col("days_traded_short_only").fill_null(0.0).pow(0.2)).alias("equilibrium"),
        ]).with_columns(  
            (pl.col("performance").fill_null(0.0).pow(0.7) *
             
             pl.col("officialR").cast(pl.Float64, strict=False).fill_null(1.0).clip(lower_bound=1).pow(0.2)
             ).alias("rating")
        )
        logger.debug("Finished calculating performance metrics on DataFrame.")
        return df


    def _update_cache_atomically(self, update_fn):
        """Performs a locked read-modify-write operation using update_fn."""
        if not self._acquire_lock():
            logger.error("Could not acquire lock for atomic cache update. Aborting.")
            return False
        success = False
        current_cache_df = None
        try:
            
            try:
                logger.debug("Loading latest cache state from disk inside atomic update...")
                current_cache_df = pl.read_csv(self.cache_main_file, ignore_errors=True, infer_schema_length=1000)
                if current_cache_df.height == 0 and os.path.exists(self.cache_main_file) and os.path.getsize(self.cache_main_file) > 0:
                    current_cache_df = pl.read_csv(self.cache_main_file, infer_schema_length=1000)
                current_cache_df = self._ensure_schema(current_cache_df)
            except (FileNotFoundError, pl.exceptions.NoDataError):
                logger.warning(f"Cache file {self.cache_main_file} not found or empty. Starting empty.")
                current_cache_df = self._create_empty_cache()
            except Exception as e:
                logger.error(f"Error loading cache file {self.cache_main_file} inside lock: {e}. Abort.", exc_info=True)
                return False 

            logger.debug("Applying update function to loaded cache...")
            modified_cache_df = update_fn(current_cache_df) 

            if modified_cache_df is not None:
                modified_cache_df = self._ensure_schema(modified_cache_df) 
                print("Cache data being written by _update_cache_atomically:")
                print(modified_cache_df)
                modified_cache_df.write_csv(self.cache_main_file)
                logger.info(f"Saved updated cache via atomic helper. Shape: {modified_cache_df.shape}")
                self.cache = modified_cache_df
                success = True
            else:
                logger.warning("Update function returned None. No changes saved.")
                self.cache = current_cache_df 
            
        except Exception as e:
            logger.error(f"Error during locked atomic cache update: {e}", exc_info=True)
            success = False
            if current_cache_df is not None: self.cache = current_cache_df
            else: self._load_cache() 
        finally:
            self._release_lock()
        return success



    
    


    def batch_set_official_stats_vectorized(self, batch_updates: list[dict]):
        updates_df = pl.DataFrame(batch_updates)
        updates_wide = updates_df.pivot(
            values="value",
            index=["Trader_Address", "typer"],
            columns="col"
        ).rename({"Trader_Address": "Trader Address", "typer": "type"})

        def batch_update_logic(df: pl.DataFrame) -> pl.DataFrame:
            updated_df = df.update(updates_wide, on=["Trader Address", "type"], how="outer")
            updated_df = self._ensure_schema(updated_df)
            updated_df = self._calculate_performance_metrics_on_df(updated_df)
            return updated_df

        self._update_cache_atomically(batch_update_logic)

    def batch_set_official_stats(self, batch_updates: list[dict]):
        """Applies multiple stat updates in a single atomic operation, with a progress bar."""
        logger.info(f"Starting batch update for {len(batch_updates)} stats.")

        def batch_update_logic(df: pl.DataFrame) -> pl.DataFrame:
            updated_cols = set()  
            for update in tqdm(batch_updates, desc="Batch updating trader stats", unit="stat"):
                Trader_Address = update['Trader_Address']
                typer = update['typer']
                col = update['col']
                value = update['value']

                if col not in df.columns:
                    logger.warning(f"Column '{col}' not found in cache. Adding.")
                    try:
                        inferred_dtype = pl.DataFrame({col: [value]}).schema[col]
                    except Exception:
                        inferred_dtype = pl.Utf8 if isinstance(value, str) else pl.Boolean if isinstance(value, bool) else pl.Float64
                    df = df.with_columns(pl.lit(None).cast(inferred_dtype).alias(col))

                trader_mask = (pl.col("Trader Address") == Trader_Address) & (pl.col("type") == typer)
                exists = df.filter(trader_mask).height > 0

                if not exists:
                    new_row_dict = self._get_default_row_dict(Trader_Address, typer)
                    new_row_df = self._ensure_schema(pl.DataFrame([new_row_dict]))
                    
                    missing_cols = [c for c in df.columns if c not in new_row_df.columns]
                    if missing_cols:
                        add_exprs = [pl.lit(None).alias(c) for c in missing_cols]
                        new_row_df = new_row_df.with_columns(add_exprs)
                    new_row_df = new_row_df.select(df.columns)
                    df = pl.concat([df, new_row_df], how="vertical_relaxed")
                
                target_dtype = df.schema[col]
                try:
                    lit_value = pl.lit(value).cast(target_dtype, strict=False)
                except Exception as e:
                    logger.error(f"Cast failed for {value} to {target_dtype} in col '{col}'. Using raw. Err: {e}")
                    lit_value = pl.lit(value)
                df = df.with_columns(pl.when(trader_mask).then(lit_value).otherwise(pl.col(col)).alias(col))

                updated_cols.add(col)
            metric_source_cols = set(self.PRESERVED_STATS_COLS) | \
                                 {c for c in self.BACKTEST_PROVIDED_COLS if 'ratio' in c or 'days' in c}
            if updated_cols & metric_source_cols:
                logger.debug("Recalculating metrics after batch updates.")
                df = self._calculate_performance_metrics_on_df(df)

            return df

        if not self._update_cache_atomically(batch_update_logic):
            logger.error("Failed to perform batch update.")



    def add_backtest(self, trader_info: List[Dict[str, str]], dataframe: pl.DataFrame, ratios: Dict[str, float],
                     modes: List[str], graph: go.Figure, write_cache_csv: bool = True) -> Tuple[Optional[go.Figure], Optional[str]]:

        print(ratios, trader_info)
        logger.info(f"Starting add_backtest for trader_info: {trader_info}")
        try:
            if isinstance(trader_info, list) and trader_info: trader_info_combined = {key: "".join([i[key] for i in trader_info]) for key in trader_info[0]}
            elif isinstance(trader_info, dict): trader_info_combined = trader_info
            else: raise ValueError(f"Invalid trader_info format: {trader_info}")
            addr = trader_info_combined.get("Trader Address")
            typ = trader_info_combined.get("type")
            if not addr or not typ: raise ValueError("Missing 'Trader Address' or 'type'")

            
            tagname = trader_info_combined.get('tagname', 'unknown')
            base_filename = f"{addr.replace('/', '_')}_{typ}_{tagname.replace('/', '_')}"
            pathname = os.path.join(self.render_dir, f"{base_filename}.csv")
            graph_path = os.path.join(self.render_dir, f"{base_filename}_graph.jpeg")

            update_data = {
                "Trader Address": addr, 
                "type": typ,           
                "tagname": tagname,    
                **ratios,              
                "file_path": pathname,
                "file_thumb_path": graph_path,
                "available": True,     
                "setup": json.dumps(trader_info),
                "modes": json.dumps(modes),
                "date_updated": time.time()
            }
            print(update_data)
            
            update_data_df = pl.DataFrame([update_data])
            logger.debug(f"Prepared update data dict for {addr}/{typ}")

        except Exception as e:
            logger.error(f"Error preparing data for add_backtest: {e}", exc_info=True)
            return None, None

        
        try:
            logger.info(f"[Simulated] Writing graph image to {graph_path}")
            graph.write_image(graph_path)
        except Exception as e: logger.error(f"Failed to write graph image to {graph_path}: {e}")
        if write_cache_csv:
            try:
                os.makedirs(os.path.dirname(pathname), exist_ok=True); dataframe.write_csv(pathname)
                logger.info(f"Wrote backtest CSV to {pathname}")
            except Exception as e: logger.error(f"Failed to write backtest CSV to {pathname}: {e}")

        
        def update_logic_for_backtest(current_df: pl.DataFrame) -> pl.DataFrame:
            trader_mask = (pl.col("Trader Address") == addr) & (pl.col("type") == typ)
            exists = current_df.filter(trader_mask).height > 0

            if exists:
                logger.debug(f"Trader {addr}/{typ} exists. Updating relevant columns.")
                updated_df = current_df.update(update_data_df, on=["Trader Address", "type"])

            else:
                logger.debug(f"Trader {addr}/{typ} does not exist. Creating new row.")
                new_row_dict = self._get_default_row_dict(addr, typ)
                
                new_row_dict.update(update_data) 
                new_row_df = self._ensure_schema(pl.DataFrame([new_row_dict]))

                
                updated_df = pl.concat([current_df, new_row_df.select(current_df.columns)], how="vertical_relaxed")

            
            logger.debug("Recalculating performance metrics after add_backtest update/insert.")
            recalculated_df = self._calculate_performance_metrics_on_df(updated_df)
            return recalculated_df

        
        if not self._update_cache_atomically(update_logic_for_backtest):
            logger.error(f"Failed to atomically update cache during add_backtest for {addr}/{typ}.")
            

        logger.info(f"Finished add_backtest for {addr}/{typ}")
        return graph, graph_path 


    def set_official_stat_on_trader(self, Trader_Address: str, typer: str, value: Any, col: str = "PNL"):
        """Sets a specific stat, handling concurrency via atomic update helper."""
        logger.debug(f"Setting stat '{col}' = {value} for {Trader_Address}/{typer} using atomic update.")
        if (Trader_Address == "0x004d415be7d9ef539e16f4a9d2e5d29c06789ece"):
            time.sleep(10)

        def update_logic_for_stat(df: pl.DataFrame) -> pl.DataFrame:
            
            if col not in df.columns:
                logger.warning(f"Column '{col}' not found in cache for set_official_stat. Adding.")
                try: inferred_dtype = pl.DataFrame({col:[value]}).schema[col]
                except Exception: inferred_dtype = pl.Utf8 if isinstance(value, str) else pl.Boolean if isinstance(value, bool) else pl.Float64
                df = df.with_columns(pl.lit(None).cast(inferred_dtype).alias(col))

            trader_mask = (pl.col("Trader Address") == Trader_Address) & (pl.col("type") == typer)
            exists = df.filter(trader_mask).height > 0

            if not exists:
                logger.info(f"Trader {Trader_Address}/{typer} not found. Creating row for set_official_stat.")
                new_row_dict = self._get_default_row_dict(Trader_Address, typer)
                new_row_dict[col] = value  
                new_row_df = pl.DataFrame([new_row_dict])

                
                missing_cols = [c for c in df.columns if c not in new_row_df.columns]
                if missing_cols:
                    add_exprs = [pl.lit(None).alias(c) for c in missing_cols]
                    new_row_df = new_row_df.with_columns(add_exprs)

                
                new_row_df = new_row_df.select(df.columns)

                
                df = pl.concat([df, new_row_df], how="vertical_relaxed")

                
                df = self._ensure_schema(df)
            else:
                target_dtype = df.schema[col]
                try: lit_value = pl.lit(value).cast(target_dtype, strict=False)
                except Exception as e: logger.error(f"Cast failed for {value} to {target_dtype} in col '{col}'. Using raw. Err: {e}"); lit_value = pl.lit(value)
                df = df.with_columns(pl.when(trader_mask).then(lit_value).otherwise(pl.col(col)).alias(col))

            
            
            metric_source_cols = set(self.PRESERVED_STATS_COLS) | \
                                 {c for c in self.BACKTEST_PROVIDED_COLS if 'ratio' in c or 'days' in c}
            if col in metric_source_cols:
                logger.debug(f"Recalculating metrics because source column '{col}' was updated by set_official_stat.")
                
                df = self._calculate_performance_metrics_on_df(df)
            return df




        if not self._update_cache_atomically(update_logic_for_stat):
            logger.error(f"Failed to atomically set stat '{col}' for {Trader_Address}/{typer}.")
            time.sleep(1)


    
    def add_to_blacklist(self, trader_info: Dict[str, str], modes: Optional[List[str]] = None):
        addr = trader_info.get("Trader Address"); typ = trader_info.get("type")
        if not addr or not typ: logger.error("'Trader Address'/'type' required for blacklist."); return
        logger.info(f"Blacklisting {addr}/{typ} using atomic update.")
        self.set_official_stat_on_trader(addr, typ, False, "available")
        
        self.set_official_stat_on_trader(addr, typ, 0.0, "performance")
        self.set_official_stat_on_trader(addr, typ, 0.0, "equilibrium")
        self.set_official_stat_on_trader(addr, typ, 0.0, "rating")


    def remove_from_blacklist(self, type: str, Trader_Address: str):
        logger.info(f"Removing {Trader_Address}/{type} from blacklist using atomic update.")
        self.set_official_stat_on_trader(Trader_Address, type, True, "available")
        

    def set_date_backtest(self, Trader_Address: str, type: str):
        self.set_official_stat_on_trader(Trader_Address, type, time.time(), "date_updated")


    
    def fetch_blacklist(self) -> List[str]:
        if self.cache is None or self.cache.height == 0: return []
        print(self.cache)
        cache_to_use = self.cache.lazy().with_columns(
            pl.lit(True).cast(pl.Boolean).alias('available') if 'available' not in self.cache.columns else pl.col('available')
        ).collect()
        return cache_to_use.filter(pl.col("available") == False)["Trader Address"].to_list()

    def fetch_blacklist_extended(self) -> List[str]:
        if self.cache is None or self.cache.height == 0: return []
        print(self.cache)
        cache_to_use = self.cache.lazy().with_columns([
            pl.lit(True).cast(pl.Boolean).alias('available') if 'available' not in self.cache.columns else pl.col('available'),
            pl.lit("").cast(pl.Utf8).alias('type') if 'type' not in self.cache.columns else pl.col('type'),
        ]).collect()
        return cache_to_use.filter(
            (pl.col("available") == False) | (pl.col("type") == "feather")
        )["Trader Address"].to_list()

    def fetch_grapha(self, setup: List[Dict[str, str]]) -> Optional[go.Figure]:
        if self.cache is None: return None
        try:
            if isinstance(setup, list) and setup:
                trader_info_combined = {key: "".join([i[key] for i in setup]) for key in setup[0]}
            elif isinstance(setup, dict):
                trader_info_combined = setup
            else:
                raise ValueError("Invalid setup format")

            trader_address = trader_info_combined.get("Trader Address", "")
            all_history_traces = []

            
            non_perp_types = ['day_history', 'week_history', 'month_history', 'allTime_history']
            perp_types = ['perpDay_history', 'perpWeek_history', 'perpMonth_history', 'perpAllTime_history']

            
            def process_history_group(history_types, group_name):
                """
                Loads, merges, and correctly normalizes a group of history files using the simplified delta method.
                """
                processed_dfs = []
                preference_order = [
                    'day_history', 'perpDay_history', 'week_history', 'perpWeek_history',
                    'month_history', 'perpMonth_history', 'allTime_history', 'perpAllTime_history'
                ]
                earliest_timestamp = None
                for hist_type in history_types:
                    history_file_path = os.path.join("..", "history_data", f"{trader_address}_{hist_type}.csv")
                    if os.path.exists(history_file_path):
                        try:
                            df = pd.read_csv(history_file_path, index_col='timestamp', parse_dates=True)
                            if earliest_timestamp is not None:
                                df = df[df.index <= earliest_timestamp]
                            earliest_timestamp = df.index.min()

                            if df.empty or 'pnl' not in df.columns:
                                continue

                            
                            df['pnl_diff'] = df['pnl'].diff()
                            
                            df.loc[df.index[0], 'pnl_diff'] = df['pnl'].iloc[0]

                            
                            processed_dfs.append(df[['pnl_diff', 'value']].assign(source=hist_type))

                        except Exception as e:
                            logger.error(f"Error loading and processing {history_file_path}: {e}")

                if not processed_dfs:
                    logger.warning(f"No data found for {group_name} group.")
                    return pd.DataFrame()

                
                master_df = pd.concat(processed_dfs)

                
                master_df['source_cat'] = pd.Categorical(master_df['source'], categories=preference_order, ordered=True)
                master_df.sort_values(['timestamp', 'source_cat'], inplace=True)
                final_df = master_df.loc[~master_df.index.duplicated(keep='last')].sort_index() 

                
                final_df['pnl'] = final_df['pnl_diff'].cumsum()

                
                final_df['value'] = pd.to_numeric(final_df['value'], errors='coerce').ffill().bfill()

                
                max_account_value = final_df['value'].max()
                if not pd.isna(max_account_value):
                    final_df['synthetic_value'] = final_df["pnl"] + max_account_value
                    initial_synthetic_value = final_df['synthetic_value'].dropna().iloc[0] if not final_df['synthetic_value'].dropna().empty else 0
                    if initial_synthetic_value != 0:
                        final_df["synthetic_value"] = final_df["synthetic_value"] / (initial_synthetic_value / 1000000)
                else:
                    final_df['synthetic_value'] = np.nan

                return final_df

            
            df_non_perp = process_history_group(non_perp_types, "Non-Perp")
            if not df_non_perp.empty:
                all_history_traces.append(go.Scatter(x=df_non_perp.index, y=df_non_perp["synthetic_value"], mode="lines", name="Synthetic Value (Non-Perp)"))
                all_history_traces.append(go.Scatter(x=df_non_perp.index, y=df_non_perp["value"], mode="lines", name="Balance (Non-Perp)"))
                all_history_traces.append(go.Scatter(x=df_non_perp.index, y=df_non_perp["pnl"], mode="lines", name="Total PNL (Non-Perp)"))

            df_perp = process_history_group(perp_types, "Perp")
            if not df_perp.empty:
                all_history_traces.append(go.Scatter(x=df_perp.index, y=df_perp["synthetic_value"], mode="lines", name="Synthetic Value (Perp)"))
                all_history_traces.append(go.Scatter(x=df_perp.index, y=df_perp["value"], mode="lines", name="Balance (Perp)"))
                all_history_traces.append(go.Scatter(x=df_perp.index, y=df_perp["pnl"], mode="lines", name="Total PNL (Perp)"))

            
            trader_mask = (pl.col("Trader Address") == trader_info_combined["Trader Address"]) & \
                          (pl.col("type") == trader_info_combined["type"])
            existing = self.cache.filter(trader_mask)

            if existing.height == 0 or not existing["available"].item(0):
                if all_history_traces:
                    fig = go.Figure(data=all_history_traces)
                    fig.update_layout(title=f"Historical Performance for {trader_address}")
                    return fig
                return None

            stuff = existing.row(0, named=True)
            modes_str = stuff.get('modes')
            existing_csv_path = stuff.get('file_path')
            if not existing_csv_path or not os.path.exists(existing_csv_path) or not modes_str: return None

            modes = json.loads(modes_str)
            df = pl.read_csv(existing_csv_path, try_parse_dates=True)
            df = df.with_columns((pl.col("date").diff().dt.total_minutes()).alias("time_diff_minutes"))
            average_time_diff = df.select(pl.col("time_diff_minutes").mean()).item() or 1440.0
            days_total = (df.height * average_time_diff) / 1440.0
            benchmark_ratios = calculate_ratios(df["benchmark"], average_time_diff, days_total) if 'benchmark' in df.columns else {}
            fig1, _ = render_performance_metrics(df, stuff, modes, benchmark_ratios, trader_name=stuff.get("tagname"))

            for trace in all_history_traces:
                fig1.add_trace(trace)

            return fig1
        except Exception as e:
            logger.error(f"Error in fetch_graph for {setup}: {e}", exc_info=True)
            return None

    def fetch_graph(self, setup: List[Dict[str, str]]) -> Optional[go.Figure]:
        if self.cache is None: return None
        try:
            if isinstance(setup, list) and setup:
                trader_info_combined = {key: "".join([i[key] for i in setup]) for key in setup[0]}
            elif isinstance(setup, dict):
                trader_info_combined = setup
            else:
                raise ValueError("Invalid setup format")

            trader_address = trader_info_combined.get("Trader Address", "")

            
            all_history_traces = []

            
            history_types = [
                'day_history', 'week_history', 'month_history', 'allTime_history',
                'perpDay_history', 'perpWeek_history', 'perpMonth_history', 'perpAllTime_history'
            ]

            
            for hist_type in history_types:
                history_file_path = os.path.join(
                    "..", "history_data", f"{trader_address}_{hist_type}.csv"
                )

                if os.path.exists(history_file_path):
                    try:
                        df_history = pd.read_csv(history_file_path)
                        if 'timestamp' not in df_history.columns:
                            logger.warning(f"Missing 'timestamp' column in {history_file_path}, using index.")
                            df_history['timestamp'] = pd.to_datetime(df_history.index)

                        df_history['timestamp'] = pd.to_datetime(df_history['timestamp']).dt.ceil(freq='5min')
                        df_history = df_history.set_index('timestamp').sort_index()

                        
                        df_history['value'] = pd.to_numeric(df_history['value'], errors='coerce').ffill().bfill()
                        df_history['pnl'] = pd.to_numeric(df_history['pnl'], errors='coerce').ffill().bfill()

                        max_account_value = df_history['value'].max() if not df_history['value'].empty else np.nan

                        if not df_history.empty and not pd.isna(max_account_value):
                            df_history['synthetic_value'] = df_history["pnl"] + max_account_value
                            
                            initial_synthetic_value = df_history['synthetic_value'].iloc[0]
                            if initial_synthetic_value and initial_synthetic_value != 0:
                                df_history["synthetic_value"] = df_history["synthetic_value"] / (initial_synthetic_value / 1000000)

                            
                            all_history_traces.append(go.Scatter(
                                x=df_history.index,
                                y=df_history["synthetic_value"],
                                mode="lines",
                                name=f"Synthetic Value ({hist_type})",
                            ))
                            all_history_traces.append(go.Scatter(
                                x=df_history.index,
                                y=df_history["value"],
                                mode="lines",
                                name=f"Balance ({hist_type})",
                            ))
                            all_history_traces.append(go.Scatter(
                                x=df_history.index,
                                y=df_history["pnl"],
                                mode="lines",
                                name=f"Total PNL ({hist_type})",
                            ))
                        logger.info(f"Successfully processed and created traces for {hist_type}.")
                    except Exception as e:
                        logger.error(f"Error processing historical data from {history_file_path}: {e}", exc_info=True)
                else:
                    logger.warning(f"No historical data found at {history_file_path}")

            
            req_cols = ["Trader Address", "type", "available", "file_path", "modes", "tagname"]
            cache_view = self.cache
            added_cols = False
            for col in req_cols:
                if col not in cache_view.columns:
                    added_cols = True
                    default, dtype = (True, pl.Boolean) if col == 'available' else ("", pl.Utf8)
                    cache_view = cache_view.with_columns(pl.lit(default).cast(dtype).alias(col))
            if added_cols: logger.warning("Added missing default columns to cache view for fetch_graph")

            trader_mask = (pl.col("Trader Address") == trader_info_combined["Trader Address"]) & (
                        pl.col("type") == trader_info_combined["type"])
            existing = cache_view.filter(trader_mask)

            if existing.height == 0 or not existing["available"].item(0): return None

            stuff = existing.row(0, named=True)
            modes_str = stuff.get('modes')
            existing_csv_path = stuff.get('file_path')
            if not existing_csv_path or not os.path.exists(existing_csv_path) or not modes_str: return None

            modes = json.loads(modes_str)
            df = pl.read_csv(existing_csv_path, try_parse_dates=True)

            if 'date' not in df.columns: return None
            if not isinstance(df['date'].dtype, pl.Datetime): df = df.with_columns(pl.col('date').str.to_datetime())

            df = df.with_columns((pl.col("date").diff().dt.total_minutes()).alias("time_diff_minutes"))
            average_time_diff = df.select(pl.col("time_diff_minutes").mean()).item() or 1440.0
            days_total = (df.height * average_time_diff) / 1440.0
            benchmark_ratios = calculate_ratios(df["benchmark"], average_time_diff,
                                                days_total) if 'benchmark' in df.columns else {}
            fig1, _ = render_performance_metrics(df, stuff, modes, benchmark_ratios, trader_name=stuff.get("tagname"))

            
            for trace in all_history_traces:
                fig1.add_trace(trace)

            return fig1
        except Exception as e:
            logger.error(f"Error in fetch_graph for {setup}: {e}", exc_info=True)
            return None


    
    
    
    
    
    
    
    
    

    
    
    
    

    
    
    
    
    
    
    
    
    
    
    

    
    
    

    

    
    
    

    
    
    
    
    

    
    
    
    

    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

    
    
    
    
    
    
    
    
    

    
    
    
    
    

    
    
    
    

    
    
    
    
    

    
    
    
    
    
    

    
    
    
    
    
    
    

    
    
    
    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    


    def retrieve_good_traders(self, perf_condition: float = 1) -> Optional[pl.DataFrame]:
        
        if self.cache is None or self.cache.height == 0: return self._create_empty_cache() 
        logger.info("Retrieving good traders based on performance criteria.")
        lazy_cache = self.cache.lazy().with_columns([
            pl.lit(True).cast(pl.Boolean).alias('available') if 'available' not in self.cache.columns else pl.col('available'),
            pl.lit(0.0).cast(pl.Float64).alias('performance') if 'performance' not in self.cache.columns else pl.col('performance'),
            pl.lit(0.0).cast(pl.Float64).alias('days_traded_normal') if 'days_traded_normal' not in self.cache.columns else pl.col('days_traded_normal'),
            pl.lit(0.0).cast(pl.Float64).alias('rating') if 'rating' not in self.cache.columns else pl.col('rating'),
        ])
        
        
        
        
        filtered_df = lazy_cache.filter(
            (pl.col("available") == True) & (pl.col("rating") > 0) &
            (((pl.col("calmar_ratio_normal") > 3) & (pl.col("sharpe_ratio_normal") > 3) & (pl.col("days_traded_normal") >  5000000)) |
             ((pl.col("calmar_ratio_normal") > 1.5) & (pl.col("sharpe_ratio_normal") > 1.5) & (pl.col("days_traded_normal") >  80000000000)))
             & (pl.col("performance") > 300000)
        ).collect()
        if filtered_df.height == 0: logger.info("No traders met criteria."); return filtered_df
        sorted_df = filtered_df.sort(by="rating", descending=True)
        if 0.0 < perf_condition < 1.0:
            cutoff_index = min(max(int(sorted_df.height * perf_condition) - 1, 0), sorted_df.height - 1)
            top_perf_rating = sorted_df.item(cutoff_index, 'rating'); logger.info(f"Rating cutoff at {perf_condition*100:.1f}%: {top_perf_rating:.4f}")
            final_df = sorted_df.filter(pl.col("rating") >= top_perf_rating)
        else: final_df = sorted_df
        logger.info(f"Retrieved {final_df.height} good traders."); return final_df


    def obtain_cache_extended(self, sorting_rule: str = 'rating') -> Optional[pl.DataFrame]:
        
        if self.cache is None or self.cache.height == 0: return self._create_empty_cache() 
        logger.debug(f"Obtaining extended cache, sorting by {sorting_rule}")
        lazy_cache = self.cache.lazy().with_columns(
            pl.lit(0.0).cast(pl.Float64).alias('days_traded_normal') if 'days_traded_normal' not in self.cache.columns else pl.col('days_traded_normal')
        )
        sort_col_exists = sorting_rule in lazy_cache.collect_schema()
        if not sort_col_exists: logger.warning(f"Sorting rule '{sorting_rule}' not in cache. Filtering only.")

        filtered_lazy = lazy_cache.filter(pl.col("days_traded_normal") > 1)
        if sort_col_exists: filtered_lazy = filtered_lazy.sort(sorting_rule, descending=True)
        return filtered_lazy.collect()


    def obtain_cache_available(self) -> Optional[pl.DataFrame]:
        
        if self.cache is None or self.cache.height == 0: return self._create_empty_cache().select(["winRate", "followPnl", "Trader Address", "tagname", "aum", "type"]) 
        logger.debug("Obtaining available cache subset.")
        required_cols = ["available", "rating", "Trader Address", "tagname", "type"]; select_cols = ["winRate", "followPnl", "Trader Address", "tagname", "aum", "type"]
        lazy_cache = self.cache.lazy(); added_cols = False
        for col in required_cols:
            if col not in self.cache.columns: added_cols = True; default_val, dtype = (True, pl.Boolean) if col == 'available' else (0.0, pl.Float64) if col == 'rating' else ("", pl.Utf8); lazy_cache = lazy_cache.with_columns(pl.lit(default_val).cast(dtype).alias(col))
        if added_cols: logger.warning("Added missing default columns to cache view for obtain_cache_available")

        current_schema = lazy_cache.collect_schema()
        if "rating" not in current_schema:
            logger.warning("Rating column missing. Returning available traders without sorting/specific columns.")
            if 'available' not in current_schema: lazy_cache = lazy_cache.with_columns(pl.lit(True).cast(pl.Boolean).alias('available'))
            final_lazy = lazy_cache.filter(pl.col("available") == True).with_columns([pl.lit(0.0).alias("aum"), pl.lit(0.0).alias("followPnl"), pl.lit(1.0).alias("winRate")])
            present_select_cols = [c for c in select_cols if c in final_lazy.collect_schema()]
            return final_lazy.select(present_select_cols).collect() 

        final_subset = lazy_cache.filter(pl.col("available") == True).sort("rating", descending=True).with_columns([pl.lit(0.0).alias("aum"), pl.lit(0.0).alias("followPnl"), pl.lit(1.0).alias("winRate")]).select(select_cols).collect()
        return final_subset




def render_performance_metrics(balance_df: pd.DataFrame, ratios: {str: float}, modes: [str], benchmark_ratios=None,
                               trader_name="") -> (go.Figure, go.Figure):
    fig = make_subplots(rows=1, cols=2)
    fig2 = make_subplots(rows=1, cols=1)
    for mode in modes:
        if mode == "normal":
            color_total = "black"
        elif mode == "long_only":
            color_total = "green"
        elif mode == "short_only":
            color_total = "red"
        else:
            color_total = "white"
        
        fig.add_trace(
            go.Scatter(
                x=balance_df["date"],
                y=balance_df[f'total_balance_{mode}'],
                mode='lines',
                name=f'Total Balance({mode})',
                line=dict(color=color_total)
            )
        )
        fig2.add_trace(
            go.Scatter(
                x=balance_df["date"],
                y=balance_df[f'total_balance_{mode}'],
                mode='lines',
                name=f'Total Balance({mode})',
                line=dict(color=color_total)
            )
        )

        
        fig.add_trace(
            go.Scatter(
                x=balance_df["date"],
                y=balance_df[f'unlocked_balance_{mode}'],
                mode='lines',
                name=f'Unlocked Balance({mode})',
                opacity=0.5,
                line=dict(color=color_total)
            )
        )
        
        ratios_current = {k.replace(f"_{mode}", ""): v for k, v in ratios.items() if k.endswith(f"_{mode}") and not k.startswith("days_traded")}
        fig.add_trace(
            go.Bar(
                x=list(ratios_current.values()),
                y=list(ratios_current.keys()),
                orientation='h',
                name=f'Performance Ratios({mode})',
                marker=dict(color=color_total)
            ),
            row=1, col=2
        )
    if benchmark_ratios is not None:
        
        fig.add_trace(
            go.Bar(
                x=list(benchmark_ratios.values()),
                y=list(benchmark_ratios.keys()),
                orientation='h',
                name='Benchmark Ratios',
                marker=dict(color="blue")
            ),
            row=1, col=2
        )

        
        fig.add_trace(
            go.Scatter(
                x=balance_df["date"],
                y=balance_df['benchmark'],
                mode='lines',
                name='Benchmark',
                line=dict(color="blue")
            )
        )

    
    fig.update_yaxes(title_text='Ratios', row=1, col=2)
    fig.update_xaxes(title_text='Values', row=1, col=2)
    
    fig.update_layout(
        title=f'Performance analysis for {trader_name}',
        xaxis_title='Date',
        yaxis_title='Value'
    )

    return fig, fig2


def render_perfcheck(balance_df: pd.DataFrame, ratios, benchmark_ratios) -> (go.Figure, go.Figure):
    fig = make_subplots(rows=1, cols=2)

    
    fig.add_trace(
        go.Scatter(
            x=balance_df["date"],
            y=balance_df['total_balance'],
            mode='lines',
            name='Account Balance'
        )
    )
    
    fig.add_trace(
        go.Scatter(
            x=balance_df["date"],
            y=balance_df['unlocked_balance'],
            mode='lines',
            name='Account Balance (Closed)'
        )
    )

    
    fig.add_trace(
        go.Scatter(
            x=balance_df["date"],
            y=balance_df['benchmark'],
            mode='lines',
            name='Benchmark'
        )
    )

    
    fig.add_trace(
        go.Bar(
            x=list(ratios.values()),
            y=list(ratios.keys()),
            orientation='h',
            name='Performance Ratios'
        ),
        row=1, col=2
    )

    
    fig.add_trace(
        go.Bar(
            x=list(benchmark_ratios.values()),
            y=list(benchmark_ratios.keys()),
            orientation='h',
            name='Benchmark Ratios'
        ),
        row=1, col=2
    )

    
    fig.update_yaxes(title_text='Ratios', row=1, col=2)
    fig.update_xaxes(title_text='Values', row=1, col=2)
    
    fig.update_layout(
        title='Account Balance and Benchmark Comparison',
        xaxis_title='Date',
        yaxis_title='Value'
    )
    fig2 = make_subplots(rows=1, cols=1)

    
    fig2.add_trace(
        go.Scatter(
            x=balance_df["date"],
            y=balance_df['total_balance'],
            mode='lines',
            name='Account Balance'
        )
    )

    return fig, fig2


def retrieve_quantile(dfr, perf_condition, column_name):
    dfr = dfr.sort(by=column_name, descending=True)
    top_20_percent_index = max(int(dfr.height * perf_condition) - 1, 0)
    return dfr[top_20_percent_index, column_name]