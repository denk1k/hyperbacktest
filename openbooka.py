FLARESOLVERR_PATH = "http://localhost:8819/"
import os
import logging
from memory_profiler import profile
import nest_asyncio
# from freqtrade.data.converter import trades_dict_to_list
import gc

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
import traceback

def compare_dataframes(df1, df2):
    differences = {}

    # Compare shapes
    if df1.shape != df2.shape:
        differences['shape'] = {
            'df1_shape': df1.shape,
            'df2_shape': df2.shape
        }
    if not df1.columns.equals(df2.columns):
        differences['columns'] = {
            # 'df1_columns': list(df1.columns),
            # 'df2_columns': list(df2.columns),
            'mismatched_columns': {
                'only_in_df1': list(set(df1.columns) - set(df2.columns)),
                'only_in_df2': list(set(df2.columns) - set(df1.columns))
            }
        }

    # Compare indices
    if not df1.index.equals(df2.index):
        differences['indices'] = {
            # 'df1_indices': list(df1.index),
            # 'df2_indices': list(df2.index),
            'mismatched_indices': {
                'only_in_df1': list(set(df1.index) - set(df2.index)),
                'only_in_df2': list(set(df2.index) - set(df1.index))
            }
        }

    # Compare cell values
    if df1.shape == df2.shape:  # Only compare values if shapes match
        unequal_cells = (df1 != df2) & ~(df1.isna() & df2.isna())
        if unequal_cells.any().any():
            differences['values'] = {
                'mismatched_cells': unequal_cells.stack().loc[lambda x: x].index.tolist(),
                'df1_values': df1.where(unequal_cells),
                'df2_values': df2.where(unequal_cells)
            }

    return differences
from datetime import datetime, timedelta, timezone
import pandas as pd
import pytz
import httpx
import requests
import tqdm.asyncio as asyntq
import polars as pl
import time
from cachetools import TTLCache, cached
import ccxt
from hlgather import get_hyperliquid_trades
import ray

def get_futures_tickers():
    # Initialize the Bybit exchange
    exchange = ccxt.bybit({
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}  # Specify 'future' for futures markets
    })

    # Load markets to ensure we have access to all trading pairs
    exchange.load_markets()

    # Filter and retrieve futures tickers
    futures_tickers = [market['symbol'] for market in exchange.markets.values() if market['symbol'].endswith("/USDT:USDT")]

    return futures_tickers


import asyncio


clock = 8


class OpenBookBase():
    pairlist_limiter = None
    margin_estimates = None
    allowed_error_count = 50
    traders = None
    curdir = None
    flagged_ud = False
    cachedir = None

    urls = {}
    last_ud = None
    session_name = None
    hl_cache = {}

    def get_unix_timestamp(self):
        return str(int(time.time() * 1000))  # str(int(datetime.now().timestamp() * 1000))

    async def make_request(self, url: str, headers, cookies, session, params, s: asyncio.Semaphore, equality_check=None,
                           retry_count=5, method="GET", suppress_errors=False, reset_code=None):
        if equality_check is None:
            equality_check = {}
        async with s:
            error_count = 0
            while error_count <= retry_count:
                try:
                    if method == "GET":
                        response = await session.get(url, headers=headers, cookies=cookies, params=params)
                    elif method == "POST":
                        response = await session.post(url, headers=headers, json=params, cookies=cookies)
                    if response.status_code == 200:
                        response_json = response.json()
                        for key, value in equality_check.items():
                            # print(url, response_json[key])
                            if not response_json[key] == value:
                                raise Exception("Equality check failed for", url)
                        return response_json
                    else:
                        raise Exception("Request failed for", url)
                except Exception as e:
                    error_count += 1
                    if error_count >= retry_count:
                        if suppress_errors:
                            return None
                        raise Exception("Retry count exceeded for ", url, "with error", e)
                    elif reset_code is not None:
                        print("RESET CODE RUNNING")
                        reset_code()

    def conv1s(self, timer):
        try:
            return pytz.timezone("Europe/Riga").localize(datetime.fromtimestamp(
                int(timer) / 1000  # TODO: throttle by one second
            )).astimezone(pytz.utc)
        except Exception as e:
            print(e)
            return None

    async def convert_tradedict(self, dataframe, predefined_traders=None, predefined_tickers=None, start_since=0):
        if predefined_tickers is None:
            predefined_tickers = []
        if predefined_traders is None:
            predefined_traders = []
        date_now = pd.to_datetime(datetime.utcnow(), utc=True).ceil("min")
        tickers_to_gen = list(dict.fromkeys(dataframe["ticker"].unique().tolist() + predefined_tickers))
        traders_to_gen = list(dict.fromkeys(dataframe["owner"].unique().tolist() + predefined_traders))
        tickers_dict = {}
        for ticker in list(dict.fromkeys(tickers_to_gen)):
            # print("Generating data for ticker ", ticker)
            dft = dataframe[dataframe["ticker"] == ticker]
            if dft.shape[0] == 0:
                df = pd.DataFrame([{"date": date_now}])
                df.set_index("date", inplace=True)
                df[["short_position_count", "long_position_count", "long_trader_count", "short_trader_count"]] = 0
                df["owner"] = ""
                for owner in traders_to_gen:
                    df[f"short_{owner}"] = 0
                    df[f"long_{owner}"] = 0
                tickers_dict[ticker] = df
                continue
            if start_since == 0:
                start_date = dft["open_time"].min() - timedelta(minutes=1)
            else:
                start_date = date_now - timedelta(days=start_since)

            dataflen = pd.date_range(start_date, date_now, freq="min")
            df = pd.DataFrame({"date": dataflen})
            df.set_index("date", inplace=True)
            df[["short_position_count", "long_position_count", "long_trader_count", "short_trader_count"]] = 0
            df["owner"] = ""
            for owner in traders_to_gen:
                # print(owner)
                df[f"short_{owner}"] = 0
                df[f"long_{owner}"] = 0
            for i, trade in dft.iterrows():
                owner = trade["owner"]
                is_short = trade["is_short"]
                open_time = trade["open_time"] if not pd.isna(trade["open_time"]) else None
                close_time = trade["close_time"] if not pd.isna(trade["close_time"]) else None

                # print(open_time, close_time)
                if not pd.isna(close_time):
                    df.loc[open_time:close_time, "owner"] = df.loc[open_time:close_time, "owner"] + f"{owner};"
                else:
                    df.loc[open_time:, "owner"] = df.loc[open_time:, "owner"] + f"{owner};"
                # print(open_time, close_time)
                if trade["is_short"]:
                    df.loc[open_time:close_time, f"short_{owner}"] += 1
                    df.loc[open_time:close_time, f"short_position_count"] += 1
                else:
                    df.loc[open_time:close_time, f"long_{owner}"] += 1
                    df.loc[open_time:close_time, f"long_position_count"] += 1
            columns_short = [f"short_{owner}" for owner in traders_to_gen]
            df["short_trader_count"] = (df[columns_short] > 0).sum(axis=1)
            columns_long = [f"long_{owner}" for owner in traders_to_gen]
            df["long_trader_count"] = (df[columns_long] > 0).sum(axis=1)
            tickers_dict[ticker] = df
            # print(df)
        return tickers_dict

    def create_folder_if_does_not_exist(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
            print("Path folder created", path)
        return path

    def auth_token_controller(self, api_url=FLARESOLVERR_PATH, session_name="tester"):
        self.api_url = api_url
        self.session_name = session_name

        logger.info(f"Latest undetectable user agent: {self.last_ud}")

    def initialize_flaresolver(self, name):
        ok = False
        while not ok:
            try:
                ok = True
                headers = {"Content-Type": "application/json"}
                data = {
                    "cmd": "sessions.create",
                    "session": name
                }
                response = httpx.post(self.api_url, headers=headers, json=data, timeout=100)
                logger.error("HIISes")
                logger.error(response.json())
                session_name = response.json()['session']
                if not response.status_code == 200:
                    ok = False
                else:
                    return session_name
            except Exception as e:
                print(traceback.format_exc())
                print(f"Exception occurred while trying to initialize sessions {e}")

    def destroy_flaresolver(self, session_name, force=False):
        ok = False
        print("Destroying flaresolver session")
        while not ok:
            try:
                headers = {"Content-Type": "application/json"}
                data = {
                    "cmd": "sessions.destroy",
                    "session": session_name
                }
                response = httpx.post(self.api_url, headers=headers, json=data)
                if response.status_code == 200 or not force:
                    ok = True
            except Exception as e:
                print(traceback.format_exc())
                print(f"Exception occurred while trying to destroy session {e}")
        return True

    def obtain_ud(self, url):
        user_agent = None
        cookies = None
        self.session_name = self.initialize_flaresolver(self.session_name)
        print(f"Initialized session: {self.session_name}")
        retries = 0
        while (user_agent is None or cookies is None) and retries < 5:
            try:
                data = {
                    "cmd": "request.get",
                    "url": url,
                    "session": self.session_name,
                    "maxTimeout": 60000
                }
                temp_headers = {"Content-Type": "application/json"}
                response = httpx.post(self.api_url, headers=temp_headers, json=data, timeout=100)
                re = response.json()
                cookies = re["solution"]["cookies"]
                cookies = {cookie["name"]: cookie["value"] for cookie in cookies}
                user_agent = re["solution"]["userAgent"]

                print(cookies, user_agent)
            except Exception as e:
                retries += 1
                print(traceback.format_exc())
                logger.exception(f"Exception occurred while trying to update session", exc_info=e)
                self.destroy_flaresolver(self.session_name)
                self.session_name = self.initialize_flaresolver(self.session_name)
                user_agent = None
        if user_agent is None:
            raise Exception("User agent is none.")
        # self.destroy_flaresolver(self.session_name)
        # logger.info("Destroyed flaresolverr session")
        last_ud = {"ua": user_agent, "cookies": cookies}
        return last_ud

    def regenerate_ud(self, url):
        # current_datetime = pd.to_datetime(datetime.utcnow(), utc=True)
        self.urls[url] = self.obtain_ud(url)
        return self.urls[url]

    def set_cache_directory(self, path):
        self.curdir = os.path.realpath(path)
        self.cachedir = self.create_folder_if_does_not_exist(path)

    def fetch_bybit_copytrading_tickers(self):
        self.headers_bybit = self.__generate_bybit_headers()
        params = {
            'filter': 'all',
        }
        response = requests.get(
            'https://api2.bybit.com/contract/v5/product/dynamic-symbol-list',
            params=params,
            cookies=self.headers_bybit[1],
            headers=self.headers_bybit[0],
        )
        if response.status_code != 200:
            return
        try:
            df = pd.DataFrame(response.json()["result"]["LinearPerpetual"])
            df = df[df["supportCopyTrade"]]
            df["symbol"] = df["symbolName"].str.replace("USDT", "/USDT:USDT")
            # df = df.filter(pl.col("supportCopyTrade") == True)
            # symbols = df.with_columns(pl.concat_str(
            # [
            #     pl.col("baseCurrency"),
            #     pl.lit("/"),
            #     pl.col("quoteCurrency"),
            #     pl.lit(":"),
            #     pl.col("quoteCurrency")
            # ],
            # separator="",
            #     ).alias("symbol"))["symbol"].to_list()

            return df["symbol"].tolist()
        except Exception as e:
            return response.json()["result"]["LinearPerpetual"]
    # def generate_ticker_relation(self, key):
    #     whitelist = self.ticker_relations
    #     coin_key = key.split("/")[0].strip("10000").strip("1000")
    #     for pair in whitelist:
    #         coin_exchange = pair.split("/")[0].strip("10000").strip("1000")
    #         if coin_exchange == coin_key:
    #             return pair
    #     return key

    def __init__(self, pairlist_limiter:list=None,cache_directory="./my_data/data/caches", flaresolverr_path = FLARESOLVERR_PATH, flaresolverr_container="tester", semaphore_compiler=4):
        self.semaphore_count = semaphore_compiler
        logger.info("HELLO")
        if pairlist_limiter is None:
            pairlist_limiter = []
        self.pairlist_limiter = pairlist_limiter
        logger.info("Pairlist assembled")
        # self.set_sources(trader_list)
        self.auth_token_controller(flaresolverr_path, flaresolverr_container)
        logger.info("Auth controller initialized")
        self.headers_okx = self.__generate_okx_headers()
        self.headers_binance = self.__generate_binance_headers()
        # self.headers_bybit = self.__generate_bybit_headers()
        logger.info("Headers generated")
        self.set_cache_directory(cache_directory)
        logger.info("Cache dir set")
        nest_asyncio.apply()


    def extend_pairlist(self, pairlist):
        self.pairlist_limiter = list(set(pairlist + self.pairlist_limiter))
        logger.error("Pairlist extended")

    async def okx_fetch_ranks(self, fetch_n=21):
        url = 'https://www.okx.com/priapi/v5/ecotrade/public/follow-rank'

        async def fetch_page(session, start):
            params = {
                'size': '20',
                'type': 'followTotalPnl',
                'start': str(start),
                'fullState': '0',
                'countryId': 'JP',
                'apiTrader': '0',
                'dataVersion': datetime.now().strftime('%Y%m%d%H%M%S'),
                't': self.get_unix_timestamp()#str(int(datetime.now().timestamp() * 1000)),
            }
            logger.info(f"Fetching page {start}")
            response = await session.get(url, params=params, headers=self.headers_okx[0])
            if not response.status_code == 200:
                raise Exception(f"Status code is not 200(OK) but {response.status_code}")
            # TODO: code response error?
            response_data = response.json()
            return pl.from_dicts(response_data["data"][0]["ranks"])

        async with httpx.AsyncClient() as session:
            tasks = [fetch_page(session, start) for start in range(1, fetch_n)]
            data_frames = []
            for task in asyncio.as_completed(tasks):
                try:
                    data_frame = await task
                    data_frames.append(data_frame)
                except Exception as e:
                    logger.exception("An error occurred while fetching data: %s", e)

            all_data = pl.concat(data_frames) if data_frames else pl.DataFrame()
            # print(all_data)
            all_data = all_data.with_columns(pl.lit("okx").alias("type"), pl.col("followPnl").cast(pl.Float64), pl.col("aum").cast(pl.Float64), pl.col("initialDay").cast(pl.Int64).alias("daysTraded"), pl.col("yieldRatio").cast(pl.Float64), pl.col("winRatio").cast(pl.Float64).alias("winRate"), pl.col("uniqueName").alias("uid"), pl.col("nickName").alias("tagname"))
            all_data = all_data.filter((pl.col("yieldRatio") > 0.4) & (pl.col("daysTraded") > 60) & (pl.col("followPnl") > 100))
            # all_data = all_data.select(["winRate", "yieldRatio", "daysTraded", "followPnl", "uid", "tagname"])
            all_data = all_data.sort("yieldRatio", descending=True)
            all_data = all_data.select(["winRate", "followPnl", "uid", "tagname", "aum", "type"])
            return all_data

    def tradedictionize(self, dataframe, predefined_traders=None, predefined_tickers=None, start_since=0,
                        multiply_leverage=False):
        # if unupdate_trades is None:
        #     unupdate_trades = []

        if predefined_tickers is None:
            predefined_tickers = []
        if predefined_traders is None:
            predefined_traders = []
        date_now = pd.to_datetime(datetime.utcnow(), utc=True).ceil("min")
        # tickers_to_gen = list(dict.fromkeys(dataframe["ticker"].unique().tolist() + predefined_tickers))
        tickers_to_gen = predefined_tickers
        traders_to_gen = list(dict.fromkeys(dataframe["owner"].unique().tolist() + predefined_traders))

        # def find_local_maxima(series, window=8000):
        #     local_maxima = series[(series.shift(window) < series) & (series.shift(-window) < series)]
        #     return local_maxima

        # if update_estimates:
        # margin_estimation = {}
        maximum_trader_margin = {}
        # for owner in traders_to_gen:
        #     dft = dataframe[dataframe["owner"] == owner]
        #     if start_since == 0:
        #         start_date = dft["open_time"].min() - timedelta(minutes=1)
        #     else:
        #         start_date = date_now - timedelta(days=start_since)
        #     dataflen = pd.date_range(start_date, date_now, freq="min")
        #     df = pd.DataFrame({"date": dataflen})
        #     df.set_index("date", inplace=True)
        #
        #     df["margin"] = 0.0
        #     for i, trade in dft.iterrows():
        #         leverage_mult = trade["leverage"] if multiply_leverage else 1
        #         open_time = trade["open_time"] if not pd.isna(trade["open_time"]) else None
        #         close_time = trade["close_time"] if not pd.isna(trade["close_time"]) else None
        #         df.loc[open_time:close_time, f"margin"] += float(trade["margin"]) * float(leverage_mult)
        #     maximum_trader_margin[owner] = df["margin"].max()

        # END
            # maxima = find_local_maxima(df["margin"])
            #
            # # Create an interpolation between the maxima
            # df['interpolated_maximum_margin'] = maxima.reindex(df.index).interpolate(method='time')
            #
            # # Fill forward and backward to cover the entire series
            # df['interpolated_maximum_margin'].fillna(method='bfill', inplace=True)
            # df['interpolated_maximum_margin'].fillna(method='ffill', inplace=True)
            # margin_estimation[owner] = df

        if self.margin_estimates is None:
            self.margin_estimates = maximum_trader_margin
        # else:
        #     for key in list(self.margin_estimates.keys()):
        #         if key in list(maximum_trader_margin.keys()) and self.margin_estimates[key] < maximum_trader_margin[key]:
        #             self.margin_estimates[key] = maximum_trader_margin[key]
        #     for key in list(maximum_trader_margin.keys()):
        #         if key not in list(self.margin_estimates.keys()):
        #             self.margin_estimates[key] = maximum_trader_margin[key]
        #
        maximum_trader_margin = self.margin_estimates.copy()

        tickers_dict = {}
        for ticker in list(dict.fromkeys(tickers_to_gen)):
            # print("Generating data for ticker ", ticker)
            dft = dataframe[dataframe["ticker"] == ticker]
            if dft.shape[0] == 0:
                df = pd.DataFrame([{"date": date_now}])
                df.set_index("date", inplace=True)
                df[["short_position_count", "long_position_count", "long_trader_count", "short_trader_count"]] = 0
                df["owner"] = ""
                for owner in traders_to_gen:
                    df[[f"short_{owner}", f"short_margin_{owner}", f"long_{owner}", f"long_margin_{owner}"]] = 0.0
                tickers_dict[ticker] = df
                continue
            if start_since == 0:
                start_date = dft["open_time"].min() - timedelta(minutes=1)
            else:
                start_date = date_now - timedelta(days=start_since)

            dataflen = pd.date_range(start_date, date_now, freq="min")
            df = pd.DataFrame({"date": dataflen})
            df.set_index("date", inplace=True)
            df[["short_position_count", "long_position_count", "long_trader_count", "short_trader_count"]] = 0
            df["owner"] = ""
            for owner in traders_to_gen:
                # print(owner)
                df[[f"short_{owner}", f"short_margin_{owner}", f"long_{owner}", f"long_margin_{owner}"]] = 0.0
                # df[f"long_{owner}"] = 0
            for i, trade in dft.iterrows():
                owner = trade["owner"]
                is_short = trade["is_short"]
                open_time = trade["open_time"] if not pd.isna(trade["open_time"]) else None
                close_time = trade["close_time"] if not pd.isna(trade["close_time"]) else None

                # print(open_time, close_time)
                if not pd.isna(close_time):
                    df.loc[open_time:close_time, "owner"] = df.loc[open_time:close_time, "owner"] + f"{owner};"
                else:
                    df.loc[open_time:, "owner"] = df.loc[open_time:, "owner"] + f"{owner};"
                # print(open_time, close_time)
                # relative_margin = margin_estimation[owner].loc[open_time, "interpolated_maximum_margin"]
                relative_margin = 1#maximum_trader_margin[owner]
                leverage_mult = trade["leverage"] if multiply_leverage else 1
                # print(margin_estimation[owner])
                # print(margin_estimation[owner].loc[open_time, "interpolated_maximum_margin"])

                if trade["is_short"]:
                    df.loc[open_time:close_time, f"short_{owner}"] += 1
                    df.loc[open_time:close_time, f"short_position_count"] += 1
                    df.loc[open_time:close_time, f"short_margin_{owner}"] += float(trade["margin"]) * float(
                        leverage_mult) / float(relative_margin)
                else:
                    df.loc[open_time:close_time, f"long_{owner}"] += 1
                    df.loc[open_time:close_time, f"long_position_count"] += 1
                    df.loc[open_time:close_time, f"long_margin_{owner}"] += float(trade["margin"]) * float(
                        leverage_mult) / float(relative_margin)
            columns_short = [f"short_{owner}" for owner in traders_to_gen]
            shortmrg = [f"short_margin_{owner}" for owner in traders_to_gen]
            df["short_trader_count"] = (df[columns_short] > 0).sum(axis=1)
            columns_long = [f"long_{owner}" for owner in traders_to_gen]
            longmrg = [f"long_margin_{owner}" for owner in traders_to_gen]
            df["long_trader_count"] = (df[columns_long] > 0).sum(axis=1)
            df["short_margin"] = (df[shortmrg]).sum(axis=1)
            df["long_margin"] = (df[longmrg]).sum(axis=1)

            tickers_dict[ticker] = df


        return tickers_dict

    async def bybit_fetch_ranks(self, fetch_n=21):
        self.headers_bybit = self.__generate_bybit_headers()
        url = 'https://api2.bybit.com/fapi/beehive/public/v1/common/dynamic-leader-list'

        async def fetch_page(session, start):
            params = {
                'timeStamp': self.get_unix_timestamp(),#str(int(datetime.now().timestamp() * 1000)),
                'pageNo': str(start),
                'pageSize': '20',
                'userTag': '',
                'dataDuration': 'DATA_DURATION_NINETY_DAY',
                'leaderTag': 'LEADER_TAG_HIGHEST_PROFIT',
                'code': '',
                'leaderLevel': '',
            }
            logger.info(f"Fetching page {start}")
            response = await session.get(url, params=params, headers=self.headers_bybit[0], cookies=self.headers_bybit[1])
            if not response.status_code == 200:
                raise Exception(f"Status code is not 200(OK) but {response.status_code}")
            # TODO: code response error?
            response_data = response.json()
            if int(response_data["retCode"]) != 0:
                raise Exception(response_data["retMsg"])
            return pl.from_dicts(response_data["result"]["leaderDetails"]).select(["leaderMark", "nickName", "metricValues"])

        async with httpx.AsyncClient() as session:
            tasks = [fetch_page(session, start) for start in range(1, fetch_n)]
            data_frames = []
            for task in asyncio.as_completed(tasks):
                try:
                    data_frame = await task
                    if data_frame is not None:
                        data_frames.append(data_frame)
                except Exception as e:
                    logger.exception("An error occurred while fetching data: %s", e)
            print(data_frames)

            all_data = pl.concat(data_frames) if len(data_frames) > 0 else pl.DataFrame()
            if len(all_data) == 0:
                return None
            all_data = all_data.with_columns(pl.col("leaderMark").alias("uid"), pl.col("nickName").alias("tagname"))
            all_data = all_data.with_columns(pl.col("metricValues").list.to_struct(
                fields=["roi", "roi_aum", "followPnl", "winRate", "stabilityIndex"])).unnest("metricValues")
            all_data = all_data.with_columns(
                pl.col("roi").str.replace(",", "", literal=True, n=100).str.replace("%", "", literal=True).str.replace(
                    "+", "", literal=True).cast(pl.Float64).truediv(100),
                pl.col("winRate").str.replace(",", "", literal=True, n=100).str.replace("%", "",
                                                                                        literal=True).str.replace("+",
                                                                                                                  "",
                                                                                                                  literal=True).cast(
                    pl.Float64).truediv(100),
                pl.col("followPnl").str.replace(",", "", literal=True, n=100).str.replace("%", "",
                                                                                          literal=True).str.replace("+",
                                                                                                                    "",
                                                                                                                    literal=True).cast(
                    pl.Float64), pl.col("roi_aum").str.replace(",", "", literal=True, n=100).str.replace("%", "",
                                                                                                         literal=True).str.replace(
                    "+", "", literal=True).cast(pl.Float64))
            all_data = all_data.with_columns(pl.col("roi_aum").truediv(pl.col("roi")).add(pl.col("roi_aum")).alias("aum"), pl.lit("bybit").alias("type"))
            all_data = all_data.filter(
                (pl.col("winRate") > 0.5) & (pl.col("followPnl") > 1000) & (pl.col("roi_aum") > 1000))
            all_data = all_data.sort("followPnl", descending=True)
            all_data = all_data.select(["winRate", "followPnl", "uid", "tagname", "aum", "type"])
            return all_data
    def regen_headers(self):
        self.headers_bybit = self.__generate_bybit_headers()

    async def bybit_check_availability(self, uid, tagname, semaphore: asyncio.Semaphore):
        self.headers_bybit = self.__generate_bybit_headers()
        url = 'https://api2.bybit.com/fapi/beehive/public/v1/common/leader-trade-record'
        params = {
            'timeStamp': self.get_unix_timestamp(),#str(int(datetime.now().timestamp() * 1000)),
            'dayCycleType': 'DAY_CYCLE_TYPE_SEVEN_DAY',
            'leaderMark': uid,
        }
        async with httpx.AsyncClient() as session:
            response_data = await self.make_request(url, params=params, headers=self.headers_bybit[0], cookies=self.headers_bybit[1], s=semaphore, session=session, equality_check={"retCode": 0})
            can_view = int(response_data["result"]["openTradeInfoProtection"]) != 1
            return uid, tagname, can_view

    async def okx_fetch_extended_trader_data(self, uid):
        params = {
            'latestNum': '0',
            'uniqueName': uid,
            't': self.get_unix_timestamp()#str(int(datetime.now().timestamp() * 1000)),
        }
        async with httpx.AsyncClient() as session:
            response = await session.get(
                'https://www.okx.com/priapi/v5/ecotrade/public/trade-data',
                params=params,
                headers=self.headers_okx[0]
            )
            if response.status_code != 200:
                raise Exception(f"status code is {response.status_code}")

            response = response.json()

            if response["code"] != "0":
                raise Exception(f"Response OKX status code is {response['code']}")

            result = response["data"][0]["centers"] + response["data"][0]["heads"] + response["data"][0]["tails"]
            result = pl.from_dicts(result)
            result = result.with_columns(pl.col("value").cast(pl.Float64)).select(["functionId", "value"])
            return result

    async def okx_fetch_extended_day_count(self, uid):
        params = {
            # 'latestNum': '0',
            'uniqueName': uid,
            't': self.get_unix_timestamp()#str(int(datetime.now().timestamp() * 1000)),
        }

        async with httpx.AsyncClient() as session:
            # response = await session.get(
            #     'https://www.okx.com/priapi/v5/ecotrade/public/yield-pnl',
            #     params=params, headers=self.headers_okx[0]
            # )
            response = await session.get(
                'https://www.okx.com/priapi/v5/ecotrade/public/total-pnl',
                params=params, headers=self.headers_okx[0]
            )
            if response.status_code != 200:
                raise Exception(f"status code is {response.status_code}")

            response = response.json()

            if response["code"] != "0":
                raise Exception(f"Response OKX status code is {response['code']}")
            print(response["data"][0]["statTime"])
            result = pl.from_dicts(response["data"])
            result = result.with_columns(
                (pl.col("ratio").ne(pl.col("ratio").shift(1))).alias("balance_changed")
            )
            days_with_changes = result.filter(pl.col("balance_changed")).height - 1
            initial_investment = 10000
            returned_graph = result.with_columns(pl.col("ratio").cast(pl.Float64).mul(initial_investment).add(initial_investment).alias("total_balance"), pl.from_epoch(pl.col("statTime").cast(pl.Int64), time_unit="ms").alias("date")).select(["date", "total_balance"])
            return days_with_changes, returned_graph








    # def limit_to_copytraded(self):
    #     pairlist = self.fetch_bybit_copytrading_tickers()
    #     self.pairlist_limiter = pairlist


    def __drop_cache(self, name, df):
        pathname = os.path.join(self.cachedir, name + ".pkl")
        df.to_pickle(pathname)
        print(f"Stored {name} cache in {pathname}")
        return pathname

    def __retrieve_cache(self, name):
        pathname = os.path.join(self.cachedir, name + ".pkl")
        if os.path.exists(pathname):
            df = pd.read_pickle(pathname)
            return df
        else:
            print(f"No df for {name} found, returning none")
            return None

    def __generate_binance_headers(self):
        self.binance_cookies = {}

        binance_headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'clienttype': 'web',
            'content-type': 'application/json',
            'csrftoken': 'add1d11d2d12b7171062847968262d1d',
            'dnt': '1',
            'lang': 'en',
            'origin': 'https://www.binance.com',
            'referer': 'https://www.binance.com/en/copy-trading/lead-details/3779422221599733504?timeRange=90D',
            'sec-ch-ua': '"Chromium";v="123", "Not:A-Brand";v="8"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'x-passthrough-token': '',
        }
        return binance_headers

    def __generate_bybit_headers(self):
        if self.flagged_ud:
            return
        self.flagged_ud = True
        try:
            self.bybit_ud = self.regenerate_ud("https://www.bybit.com/copyTrade/")

        except Exception as e:
            logger.error(traceback.format_exc())
        finally:
            self.flagged_ud = False

        print("REGENERATED HEADERS")
        headers_bybit = {
            "Sec-Fetch-Mode": "no-cors",
            "User-Agent": self.bybit_ud["ua"],
            "TE": "trailers",
            "Sec-Fetch-Site": "same-site",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Origin": "https://www.bybit.com",
            "Referer": "https://www.bybit.com/",
            "DNT": "1",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "Sec-Fetch-Dest": "empty",
            "Host": "api2.bybit.com",
        }
        self.headers_bybit = [headers_bybit, self.bybit_ud["cookies"]]
        return self.headers_bybit

    def __generate_okx_headers(self):
        self.headers_okx = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'app-type': 'web',
            'cookie': 'locale=en_US; preferLocale=en_US; ok_prefer_udColor=0; ok_prefer_udTimeZone=0; browserVersionLevel=v5.6ad2a8e37c01; devId=c79ad69f-b9d8-42ca-9148-e25d2d91674d; OptanonAlertBoxClosed=2024-04-16T17:57:13.992Z; intercom-id-ny9cf50h=36027190-7598-46c9-801e-6fe9e1a58ec8; intercom-device-id-ny9cf50h=21366273-b7fa-4de5-9b66-c75fe34345b8; amp_56bf9d=c79ad69f-b9d8-42ca-9148-e25d2d91674d...1hti0utnh.1hti0vthp.68.0.68; ok_site_info===Qf3ojI5RXa05WZiwiIMFkQPx0Rfh1SPJiOiUGZvNmIsIiVMJiOi42bpdWZyJye; first_ref=https%3A%2F%2Fwww.okx.com%2Fcopy-trading%2Faccount%2FB0D4CDE722DB850E%3Ftab%3Dswap; fingerprint_id=c79ad69f-b9d8-42ca-9148-e25d2d91674d; __cf_bm=KnXKMt8UzZkypDe1slOXK5SFPJy0vFJbugM8fRmmim4-1717177807-1.0.1.1-dcW52Nx48AyZq6mlsSDcU7KSK9ft.HOVTsWRmdYKwHrG1t1j7EHhutLOvxNi9Rvk.nZYie.GlmLWftWmG3Nxnw; OptanonConsent=isGpcEnabled=0&datestamp=Fri+May+31+2024+20%3A50%3A07+GMT%2B0300+(Eastern+European+Summer+Time)&version=202212.1.0&isIABGlobal=false&hosts=&consentId=b80b34cc-df5d-40e1-a473-3388906d1899&interactionCount=1&landingPath=NotLandingPage&groups=C0004%3A0%2CC0002%3A0%2CC0001%3A1%2CC0003%3A0&geolocation=LV%3BRIX&AwaitingReconsent=false; okg.currentMedia=sm; ok-ses-id=P91+vWHN9SWDOKVUnhl6iS33b17G4UrJVPdwshSA06Q6D6bltyw3TOk+GyiXId9aYun+5bWSHRAMUzBJf0YQds4fD/FZJnxy8J744VEw/p81SB67QVkAkif2VP195Zy3; traceId=1020171778151390003; ok_prefer_currency=%7B%22currencyId%22%3A7%2C%22isDefault%22%3A1%2C%22isPremium%22%3Afalse%2C%22isoCode%22%3A%22EUR%22%2C%22precision%22%3A2%2C%22symbol%22%3A%22%E2%82%AC%22%2C%22usdToThisRate%22%3A0.923%2C%22usdToThisRatePremium%22%3A0.923%2C%22displayName%22%3A%22EUR%22%7D; _monitor_extras={"deviceId":"E3u7Ij2A-XxDMaO9u3wRdS","eventId":727,"sequenceNumber":727}',
            'devid': 'c79ad69f-b9d8-42ca-9148-e25d2d91674d',
            'dnt': '1',
            'priority': 'u=1, i',
            'referer': 'https://www.okx.com/copy-trading/account/2BE980C9BEA40361?tab=swap',
            'sec-ch-ua': '"Chromium";v="125", "Not.A/Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'x-cdn': 'https://www.okx.com',
            'x-id-group': '1030771778070800004-c-31',
            'x-locale': 'en_US',
            'x-site-info': '==Qf3ojI5RXa05WZiwiIMFkQPx0Rfh1SPJiOiUGZvNmIsIiVMJiOi42bpdWZyJye',
            'x-utc': '3',
            'x-zkdex-env': '0',
        }
        return [self.headers_okx]

    @cached(cache=TTLCache(maxsize=1000, ttl=clock))

    async def __discord_get_current_data(self, session, uid, suppress_errors=False, get_n=1):
        cookies = {}

        headers = {
            'authority': 'discord.com',
            'accept': '*/*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'authorization': 'fill authorization here',
            'referer': 'https://discord.com/channels/972414672584183828/1158497806777065554',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.199 Safari/537.36',
            'x-debug-options': 'bugReporterEnabled',
            'x-discord-locale': 'en-US',
            'x-discord-timezone': 'Europe/Riga',
            'x-super-properties': 'eyJvcyI6Ik1hYyBPUyBYIiwiYnJvd3NlciI6IkNocm9tZSIsImRldmljZSI6IiIsInN5c3RlbV9sb2NhbGUiOiJlbi1HQiIsImJyb3dzZXJfdXNlcl9hZ2VudCI6Ik1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwXzE1XzcpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS8xMTkuMC42MDQ1LjE5OSBTYWZhcmkvNTM3LjM2IiwiYnJvd3Nlcl92ZXJzaW9uIjoiMTE5LjAuNjA0NS4xOTkiLCJvc192ZXJzaW9uIjoiMTAuMTUuNyIsInJlZmVycmVyIjoiaHR0cHM6Ly93d3cub3BlcmEuY29tLyIsInJlZmVycmluZ19kb21haW4iOiJ3d3cub3BlcmEuY29tIiwicmVmZXJyZXJfY3VycmVudCI6Imh0dHBzOi8vZGlzY29yZC5jb20vIiwicmVmZXJyaW5nX2RvbWFpbl9jdXJyZW50IjoiZGlzY29yZC5jb20iLCJyZWxlYXNlX2NoYW5uZWwiOiJzdGFibGUiLCJjbGllbnRfYnVpbGRfbnVtYmVyIjoyNTYyMzEsImNsaWVudF9ldmVudF9zb3VyY2UiOm51bGx9',
        }
        df_all = pd.DataFrame()
        df_msg = pd.DataFrame()
        limit = 50
        params = {
            'limit': str(limit),
        }
        n = 0
        msg_current = (await session.get(f'https://discord.com/api/v9/channels/{uid}/messages',
                                         params=params, cookies=cookies, headers=headers)).json()
        last_trade_id = 0
        while len(msg_current) > 1 and n < get_n:
            n += 1
            for message in msg_current:
                if len(message["embeds"]) > 0:
                    dfnew = message["embeds"][0]["fields"]
                    dictionary_this = pd.DataFrame([{iq["name"]: iq["value"] for iq in dfnew} | {
                        "title": message["embeds"][0]["title"], "timestamp": message["timestamp"]}])
                    dictionary_this["timestamp"] = pd.to_datetime(dictionary_this["timestamp"])
                    dictionary_this.set_index("timestamp", inplace=True)
                    df_msg = df_msg.combine_first(dictionary_this)
                    # trade_id = int(dictionary_this["Trade ID"])
            if n < get_n:
                params["before"] = msg_current[-1]["id"]
                if n < get_n:
                    msg_current = (await session.get(f'https://discord.com/api/v9/channels/{uid}/messages',
                                                     params=params, cookies=cookies, headers=headers)).json()
        for i, row in df_msg.iterrows():
            trade_id = int(row["Trade ID"])
            if "entry_fill" in row["title"]:
                print("Entering", trade_id)
                if last_trade_id > trade_id:
                    df_all = df_all.head(0)
                df_this = pd.DataFrame([row])
                df_this["is_short"] = df_this["Direction"] == "Short"
                # df_this["close_time"] = pd.to_datetime(df_this["Close date"], utc=True).dt.floor("min")
                df_this["open_time"] = pd.to_datetime(df_this["Open date"], utc=True).dt.floor("min")
                df_this["close_time"] = None
                df_this["leverage"] = 1
                df_this["ticker"] = df_this["Pair"].apply(
                    lambda x: x if x.endswith(":USDT") else x.replace("/USDT", "/USDT:USDT"))
                df_this["owner"] = uid
                df_this.rename(columns={"Close rate": "close_price", "Open rate": "open_price", "Amount": "margin",
                                        "Trade ID": "tradeItemId"},
                               inplace=True)
                df_this["tradeItemId"] = df_this["tradeItemId"].astype(int)
                df_this.set_index("tradeItemId", inplace=True)
                df_all = df_all.combine_first(df_this)
                last_trade_id = trade_id
            elif "exit_fill" in row["title"]:
                if trade_id in df_all.index.values:
                    print("Exiting", trade_id)
                    df_all.drop(index=trade_id, inplace=True)
        #     # messages_df = pd.DataFrame()
        #
        #     # dict_all = []
        #     # for message in msg_current:
        #     #     if len(message["embeds"]) > 0:
        #     #         dfnew = message["embeds"][0]["fields"]
        #     #         trade_exiting = "exit_fill" in message["embeds"][0]["title"]
        #     #         dictionary_this = {iq["name"]: iq["value"] for iq in dfnew}
        #     #         trade_id = int(dictionary_this["Trade ID"])# | {"title": message["embeds"][0]["title"]}
        #
        #
        # print("SIGMATIC SOFTWARE ASSOCIATION")
        # print(df_all)
        # return await self.generate_dummy("Discord")
        return df_all

    async def __discord_get_past_data(self, session, uid=1158497806777065554,
                                      suppress_errors=False):  # TODO: VERY LOW QUALITY CODE
        cookies = {}

        headers = {
            'authority': 'discord.com',
            'accept': '*/*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'authorization': 'fill authorization here',
            'referer': 'https://discord.com/channels/972414672584183828/1158497806777065554',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.199 Safari/537.36',
            'x-debug-options': 'bugReporterEnabled',
            'x-discord-locale': 'en-US',
            'x-discord-timezone': 'Europe/Riga',
            'x-super-properties': 'eyJvcyI6Ik1hYyBPUyBYIiwiYnJvd3NlciI6IkNocm9tZSIsImRldmljZSI6IiIsInN5c3RlbV9sb2NhbGUiOiJlbi1HQiIsImJyb3dzZXJfdXNlcl9hZ2VudCI6Ik1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwXzE1XzcpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS8xMTkuMC42MDQ1LjE5OSBTYWZhcmkvNTM3LjM2IiwiYnJvd3Nlcl92ZXJzaW9uIjoiMTE5LjAuNjA0NS4xOTkiLCJvc192ZXJzaW9uIjoiMTAuMTUuNyIsInJlZmVycmVyIjoiaHR0cHM6Ly93d3cub3BlcmEuY29tLyIsInJlZmVycmluZ19kb21haW4iOiJ3d3cub3BlcmEuY29tIiwicmVmZXJyZXJfY3VycmVudCI6Imh0dHBzOi8vZGlzY29yZC5jb20vIiwicmVmZXJyaW5nX2RvbWFpbl9jdXJyZW50IjoiZGlzY29yZC5jb20iLCJyZWxlYXNlX2NoYW5uZWwiOiJzdGFibGUiLCJjbGllbnRfYnVpbGRfbnVtYmVyIjoyNTYyMzEsImNsaWVudF9ldmVudF9zb3VyY2UiOm51bGx9',
        }
        df_all = pd.DataFrame()
        limit = 50
        params = {
            'limit': str(limit),
        }
        msg_current = (await session.get(f'https://discord.com/api/v9/channels/{uid}/messages',
                                         params=params, cookies=cookies, headers=headers)).json()
        while len(msg_current) > 1:
            dict_all = []
            for message in msg_current:
                if len(message["embeds"]) > 0 and 'exit_fill' in message["embeds"][0]["title"]:
                    dfnew = message["embeds"][0]["fields"]
                    dictionary_this = {iq["name"]: iq["value"] for iq in dfnew}
                    dict_all.append(dictionary_this)
            df = pd.DataFrame(dict_all)
            if df.shape[0] > 0:
                df["tradeItemId"] = (pd.to_datetime(df["Open date"], utc=True).dt.floor("min")).astype(int) + df[
                    "Trade ID"].astype(int)
                df.set_index("tradeItemId", inplace=True)
                # print(df)
                df_all = df_all.combine_first(df)
                params["before"] = msg_current[-1]["id"]
                msg_current = (await session.get(f'https://discord.com/api/v9/channels/{uid}/messages',
                                                 params=params, cookies=cookies, headers=headers)).json()
            else:
                msg_current = []
        df_all["is_short"] = df_all["Direction"] == "Short"
        df_all["close_time"] = pd.to_datetime(df_all["Close date"], utc=True).dt.floor("min")
        df_all["open_time"] = pd.to_datetime(df_all["Open date"], utc=True).dt.floor("min")
        df_all["leverage"] = 1
        df_all["ticker"] = df_all["Pair"].apply(
            lambda x: x if x.endswith(":USDT") else x.replace("/USDT", "/USDT:USDT"))
        df_all["owner"] = uid
        df_all.rename(columns={"Close rate": "close_price", "Open rate": "open_price", "Amount": "margin"},
                      inplace=True)
        # df_all["tradeItemId"] = df_all["Open date"] + df_all["Close date"]
        # df_all.set_index("tradeItemId", inplace=True)
        final = df_all[["leverage", "open_price", "ticker", "open_time", "margin", "is_short", "owner", "close_time",
                        "close_price"]]
        # print(final)
        return final

    def generate_dummy(self, source: str):
        df = pd.DataFrame(
            columns=["leverage", "open_price", "ticker", "open_time", "margin", "is_short", "owner", "close_time",
                     "close_price", "tradeItemId"])
        df.set_index("tradeItemId", inplace=True)
        return df

    async def bybit_retry(self, url, session, generate_params, starting_cursor=1):

        # print(headers)
        retries = 0
        response_json = None
        # response = None

        while retries < 3 and response_json is None:
            try:
                headers = self.headers_bybit
                response = await session.get(url, params=generate_params(starting_cursor), headers=headers[0], cookies=headers[1])
                temp_resp = response.json()
                if response.status_code == 200 and temp_resp["retMsg"] == "success":
                    response_json = temp_resp
                else:
                    raise Exception(f"Status code not 200, {temp_resp['retMsg']}")
            except Exception as e:
                retries += 1
                print(traceback.format_exc())
                self.headers_bybit = self.__generate_bybit_headers()

                print("New headers: ", self.headers_bybit)
                print("Exception on retry at bybit current data", e)
        return response_json


    @cached(cache=TTLCache(maxsize=1000, ttl=clock))
    async def __bybit_get_current_data(self, session, uid, suppress_errors=False):

        headers = self.headers_bybit
        pagesize = 50

        def generate_params(pagen):
            current_unix = self.get_unix_timestamp()#int(datetime.utcnow().strftime("%s")) * 1000
            params = {
                'timeStamp': str(current_unix),
                'page': pagen,
                'pageSize': pagesize,
                'leaderMark': uid,
            }
            # print(f"Adding {pagen}th page to request")
            return params

        url = "https://api2.bybit.com/fapi/beehive/public/v1/common/order/list-detail"
        response_json = await (self.bybit_retry(url, session, generate_params))
        # retries = 0
        # response_json = None
        # response = None
        #
        # while retries < 3 and response_json is None:
        #     try:
        #         self.headers_bybit = self.__generate_bybit_headers()
        #         response = await session.get(url, params=generate_params(1), headers=headers[0], cookies=headers[1])
        #         if response.status_code == 200 and response_json["retMsg"] == "success":
        #             response_json = response.json()
        #     except Exception as e:
        #         print("Exception on retry at bybit current data")
        if response_json is not None:
            df = pd.DataFrame(response_json["result"]["data"])
            # print(df)
            if df.shape[0] > 0:
                # print(df)
                df["tradeItemId"] = df["crossSeq"].astype(str) + '-' + df["entryPrice"].astype(str) + '-' + df[
                    "symbol"].astype(str) + '-' + df["leverageE2"].astype(str) + '-' + df["side"].astype(str) + '-' + df[
                                        "createdAtE3"].astype(str) + '-' + df["transactTimeE3"].astype(str) + '-' + df["orderCostE8"].astype(str)+ '-' + df["closeFreeQtyX"].astype(str)
                df.set_index("tradeItemId", inplace=True)
                max_trades = int(response_json["result"]["totalCount"])
                page_count = max_trades // pagesize + 1
                s = asyncio.Semaphore(10)
                tasks = [
                    self.make_request(url, headers[0], headers[1], session, generate_params(i + 1), s, {"retMsg": "success"},
                                 self.allowed_error_count, reset_code=None) for i in range(page_count) if i > 0]
                got_no_fluff = df.shape[0]
                for cor in asyncio.as_completed(tasks):
                    response_json = await cor
                    if response_json is None:
                        raise Exception(f"There was an error in make_request for bybit current data on {uid}")
                    newdf = pd.DataFrame(response_json["result"]["data"])
                    # print(newdf)
                    if newdf.shape[0] > 0:
                        newdf["tradeItemId"] = newdf["crossSeq"].astype(str) + '-' + newdf["entryPrice"].astype(str) + '-' + \
                                               newdf["symbol"].astype(str) + '-' + newdf["leverageE2"].astype(str) + '-' + \
                                               newdf["side"].astype(str) + '-' + newdf["createdAtE3"].astype(str) + '-' + newdf["transactTimeE3"].astype(str) + '-' + newdf["orderCostE8"].astype(str) + '-' + newdf["closeFreeQtyX"].astype(str)
                        got_no_fluff += newdf.shape[0]
                        newdf.set_index("tradeItemId", inplace=True)
                        df = df.combine_first(newdf)
                if not df.shape[0] == max_trades:
                    logger.warning(f"Trade count mismatch: {df.shape[0]} trades received, exchange says it should be {max_trades}")
                df["open_time"] = pd.to_datetime(df["createdAtE3"].astype(int),unit='ms', utc=True).dt.ceil("min")
                df["close_time"] = None
                df["open_price"] = df["entryPrice"].astype(float)
                df["close_price"] = None
                df["margin"] = (df["sizeX"].astype(float) / (10**8)) * (df["entryPrice"].astype(float)) / (df["leverageE2"].astype(float)/(10**2))#df["orderCostE8"].astype(float) / (10**8)#df["size"].astype(float) * df["entryPrice"].astype(float)  # TODO: unify margin params
                df["ticker"] = df["symbol"].str.replace("USDT", "/USDT:USDT").replace("1000", "")
                df["is_short"] = df["side"] == "Sell"
                df["owner"] = uid
                df["leverage"] = df["leverageE2"].astype(int) / (10 ** 2)
                df = df[["leverage", "open_price", "ticker", "open_time", "margin", "is_short", "owner", "close_time",
                         "close_price"]]
            else:
                df = self.generate_dummy(uid)
            return df
        elif suppress_errors:
            print(f"FATAL: Error on loop start for bybit request at {uid}, {response_json['retMsg']}")
            return self.generate_dummy(uid)
        else:
            raise Exception(f"Exception on loop start for bybit request at {uid}")

    @cached(cache=TTLCache(maxsize=1000, ttl=clock))

    async def __binance_get_current_data(self, session, uid, get_n=0, suppress_errors=False, shift_minutes=1):
        # return self.generate_dummy('binance-test')
        params = {
            'portfolioId': uid,
        }
        url = 'https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-data/positions'
        response = await session.get(url, params=params, cookies=self.binance_cookies, headers=self.headers_binance)
        if response.status_code == 200:
            response_json = response.json()
            if not response_json["code"] == "000000":
                raise Exception("response code bad")
            if len(response_json["data"]) == 0:
                logger.info(f"No positions available for {uid}")
                return self.generate_dummy(f"binance-{uid}")
            df_all = pd.DataFrame(response.json()["data"])
            # if df_all.shape[0] == 0:
            #     return self.generate_dummy(f"binance-{uid}")
            df_all["positionAmount"] = df_all["positionAmount"].astype(float)
            df_all = df_all[df_all["positionAmount"] > 0]
            df_all["tradeItemId"] = df_all["id"] + uid
            df_all.set_index("tradeItemId", inplace=True)
            previous_trades = self.__retrieve_cache(uid)
            if previous_trades is not None:
                df_all["open_time"] = pd.to_datetime(datetime.utcnow(), utc=True).ceil("min")
                for i, trade in df_all.iterrows():
                    for ib, tradeb in previous_trades.iterrows():
                        if ib == i:
                            df_all.loc[i, "open_time"] = max(
                                pd.to_datetime(datetime.utcnow(), utc=True).floor("min") - timedelta(
                                    minutes=shift_minutes), tradeb["open_time"])
            else:
                df_all["open_time"] = pd.to_datetime(datetime.utcnow(), utc=True).floor("min") - timedelta(
                    minutes=shift_minutes)
            df_all["close_time"] = None
            df_all["ticker"] = df_all["symbol"].apply(
                lambda x: x.replace("USDT", "/USDT:USDT") if not x.endswith("/USDT:USDT") else x)
            df_all["close_price"] = None
            df_all["owner"] = uid
            df_all["is_short"] = df_all["positionSide"].str.upper() == "SHORT"
            df_all.rename(columns={"entryPrice": "open_price", "positionAmount": "margin"}, inplace=True)
            df_all = df_all[
                ["leverage", "open_price", "ticker", "open_time", "margin", "is_short", "owner", "close_time",
                 "close_price"]]
            self.__drop_cache(uid, df_all)
            # print(df_all, "current trades obtained", previous_trades is not None)
            return df_all

        else:
            if not suppress_errors:
                raise Exception("Exception on loop start for binance request")
            else:
                print("Exception on loop start for binance request")
                return None

    async def __binance_get_past_data(self, session, uid='3695845579071974401', get_n=0, suppress_errors=True):
        pagesize = 50

        def generate_params(page_number):
            json_data = {
                'pageNumber': page_number,
                'pageSize': pagesize,
                'portfolioId': uid,
            }
            # print(json_data)
            return json_data

        url = 'https://www.binance.com/bapi/futures/v1/public/future/copy-trade/lead-portfolio/position-history'
        response = await session.post(url, headers=self.headers_binance, json=generate_params(1),
                                      cookies=self.binance_cookies)
        response_json = response.json()
        # print(response_json)
        if response.status_code == 200 and response_json["code"] == '000000':
            max_trades = int(response_json["data"]["total"])
            page_count = min(max_trades // pagesize + 1, get_n + 1)
            s = asyncio.Semaphore(2)
            needed_trades = min(page_count * pagesize, max_trades)
            df = pd.DataFrame(response_json["data"]["list"])
            df["tradeItemId"] = df["symbol"] + df["id"].astype(str)
            # df.rename(columns={"id": "tradeItemId"}, inplace=True)
            df.set_index('tradeItemId', inplace=True)
            tasks = [
                self.make_request(url, self.headers_binance, self.binance_cookies, session, generate_params(i + 1), s,
                             {"code": "000000"},
                             self.allowed_error_count, "POST") for i in range(page_count)]
            for cor in asyntq.tqdm.as_completed(tasks):
                response_json = await cor
                if not int(response_json["data"]["total"]) == max_trades:
                    return None
                newdf = pd.DataFrame(response_json["data"]["list"])
                try:
                    newdf["tradeItemId"] = newdf["symbol"] + newdf["id"].astype(str)
                    # newdf["tradeItemId"] = newdf["crossSeq"].astype(str) + '-' + newdf["entryPrice"].astype(str) + '-' + newdf["symbol"].astype(str)  + '-' + newdf["leverageE2"].astype(str) + '-' + newdf["side"].astype(str) + '-' + newdf["startedTimeE3"].astype(str)
                    newdf.set_index("tradeItemId", inplace=True)
                    df = df.combine_first(newdf)
                except Exception as e:
                    if not suppress_errors:
                        raise Exception("could not compute trades", e)
                    else:
                        print("WARNING: ", e)
            if df.shape[0] < needed_trades:
                if not suppress_errors:
                    raise Exception("FUCKIT" + str(df.shape[0]) + " " + str(needed_trades))
                else:
                    print("WARNINGPAST: " + str(df.shape[0]) + " " + str(needed_trades))
            # print(df, needed_trades, max_trades)
            df["ticker"] = df["symbol"].apply(
                lambda x: x.replace("USDT", "/USDT:USDT") if not x.endswith("/USDT:USDT") else x)
            df["leverage"] = 1  # LEVERAGE UNKNOWN?
            df["is_short"] = df["side"] == "Short"
            df.dropna(subset=["opened", "closed"], inplace=True)
            df["open_time"] = df["opened"].apply(self.conv1s).dt.ceil("min")
            df["close_time"] = df["closed"].apply(self.conv1s).dt.ceil("min")
            df["owner"] = uid
            df.rename(columns={"avgCost": "open_price", "avgClosePrice": "close_price"}, inplace=True)
            # print("BEfore margin")
            df["margin"] = df["closedVolume"] * df["open_price"]

            df = df[["leverage", "open_price", "ticker", "open_time", "margin", "is_short", "owner", "close_time",
                     "close_price"]]
            # print(df)
            return df
        else:
            # print(response_json)
            raise Exception(f"Exception on loop start for bybit request at {uid}")

    async def __table_get_past_data(self, session, uid, get_n=0):
        df = pd.read_pickle(os.path.join(self.cachedir, uid + ".pkl"))
        df["owner"] = uid
        return df

    async def __table_get_current_data(self, session, uid):
        return self.generate_dummy("Table")


    async def __bybit_get_past_data(self, session, uid, get_n=0, suppress_errors=False, delay=4):
        past_cache_fn = os.path.join(self.cachedir, f"bybit_{uid}.pkl".replace('/', '_'))
        logger.info(f"Loading in previous dataframe from {past_cache_fn}")
        df_trades_prev = pd.DataFrame() if not os.path.exists(past_cache_fn) else pd.read_pickle(past_cache_fn)
        #TODO CRITICAL: NOTICE WE CAN ONLY USE DOCKERIZED CONTAINERS THAT ARE ON UTC TIME....
        # df_trades_prev = pd.DataFrame()
        pagesize = 50
        # headers = self.__generate_bybit_headers()
        current_unix = self.get_unix_timestamp()#int(datetime.utcnow().strftime("%s")) * 1000
        next_available = True

        def generate_params(cursor=""):
            params = {
                'timeStamp': str(current_unix),
                'pageAction': "first_page" if cursor=="" else "next",
                'pageSize': pagesize,
                'leaderMark': uid,
            } | ({} if cursor == "" else {"cursor": cursor})
            return params
        cursor = ""
        df = pd.DataFrame()
        url = 'https://api2.bybit.com/fapi/beehive/public/v1/common/leader-history'
        error_count = 0
        while next_available:
            logger.info(f"Fetching {uid}@{cursor} cursor")
            # params = generate_params(cursor)
            response_json = await self.bybit_retry(url, session, generate_params, starting_cursor=cursor)
            # response = await session.get(url, params=params, headers=headers[0], cookies=headers[1])
            # response_json = response.json()
            if response_json is not None:#response.status_code == 200 and response_json["retMsg"] == "success":
                next_available = response_json["result"]["hasNext"]
                cursor = response_json["result"]["cursor"]
                newdf = pd.DataFrame(response_json["result"]["data"])
                if newdf.shape[0] == 0:
                    next_available = False
                    continue
                newdf["tradeItemId"] = newdf["orderId"]
                newdf.set_index("tradeItemId", inplace=True)
                if df_trades_prev.shape[0] > 0:
                    common_indices = newdf.index.intersection(df_trades_prev.index)
                    if not common_indices.empty:
                        logger.info("Stopping procedural fetch as common indices are not empty - reached the fetched data points.")
                        next_available = False
                df = df.combine_first(newdf)
            else:
                error_count += 1
                if error_count >= 5 and suppress_errors:
                    break
                print(f"ERROR AT BYBIT({uid}): ", response_json)
        if df.shape[0] == 0:
            return df_trades_prev
        df["open_time"] = pd.to_datetime(df["startedTimeE3"].astype(int), unit='ms', utc=True).dt.ceil("min") + timedelta(minutes=delay)
        df["close_time"] = pd.to_datetime(df["closedTimeE3"].astype(int), unit='ms', utc=True).dt.ceil("min") + timedelta(minutes=delay)
        df["open_price"] = df["entryPrice"].astype(float)
        df["close_price"] = df["closedPrice"].astype(float)
         # TODO: unify margin params
        df["ticker"] = df["symbol"].str.replace("USDT", "/USDT:USDT").replace("1000", "")
        df["is_short"] = df["side"] == "Sell"
        df["owner"] = uid
        df["leverage"] = df["leverageE2"].astype(int) / (10**2)
        df["margin"] = df["size"].astype(float) * df["entryPrice"].astype(float) / (
                df["leverageE2"].astype(float) / (10 ** 2))
        df = df[["leverage", "open_price", "ticker", "open_time", "margin", "is_short", "owner", "close_time",
                 "close_price"]]
        logger.info(f"Loading in previous dataframe with {df_trades_prev.shape[0]} trades")
        logger.info(f"Current dataframe has {df.shape[0]} trades")
        if df_trades_prev.shape[0] > 0:
            df = df_trades_prev.combine_first(df)

        # df["margin"] = df["size"].astype(float) * df["entryPrice"].astype(float) / (
        #             df["leverageE2"].astype(float) / (10 ** 2))
        logger.info(f"Merged dataframe has {df.shape[0]} trades")
        df.to_pickle(past_cache_fn)
        return df

    async def __okx_get_past_data(self, session, uid, get_n=0):
        past_cache_fn = os.path.join(self.cachedir, f"okx_{uid}.pkl".replace('/', '_'))
        logger.info(f"Loading in previous dataframe from {past_cache_fn}")
        df_trades_prev = pd.DataFrame() if not os.path.exists(past_cache_fn) else pd.read_pickle(past_cache_fn)
        current_unix = self.get_unix_timestamp()#int(datetime.utcnow().strftime("%s")) * 1000
        params = {"t": current_unix, "uniqueName": uid, "size": 200}
        url = f"https://www.okx.com/priapi/v5/ecotrade/public/position-history"
        response = await session.get(url, params=params, timeout=30, headers=self.headers_okx[0])
        if response.status_code == 200 and int(response.json()["code"]) == 0:
            df_trades = pd.DataFrame(response.json()["data"]).set_index("tradeItemId")
            current_length = df_trades.shape[0]
            past_length = 0
            error_count = 0
            got = 0
            # print("Fetching parst")
            while current_length > past_length and get_n > got:  # TODO will bug if trade_count:::200
                last_result_id = df_trades["id"].astype(int).min()
                params = {"t": current_unix, "uniqueName": uid, "size": 200, "after": last_result_id}
                response = await session.get(url, params=params, timeout=30, headers=self.headers_okx[0])
                if response.status_code == 200 and int(response.json()["code"]) == 0:
                    past_length = current_length
                    testdf = pd.DataFrame(response.json()["data"])
                    if testdf.shape[0] > 0:
                        testdf = testdf.set_index("tradeItemId")
                        if df_trades_prev.shape[0] > 0:
                            common_indices = testdf.index.intersection(df_trades_prev.index)
                            if not common_indices.empty:
                                logger.info(
                                    "Stopping procedural fetch as common indices are not empty - reached the fetched data points.")
                                get_n = got
                        df_trades = df_trades.combine_first(testdf)
                        current_length = df_trades.shape[0]
                    got += 1
                else:
                    error_count += 1
                    if error_count >= self.allowed_error_count:
                        print("Too many errors occurred while acquiring past data")
                        raise Exception("Too many errors")
            df_trades["is_short"] = False
            df_trades.loc[df_trades["posSide"] == "short", "is_short"] = True
            df_trades = df_trades[(df_trades["uTime"] != "") & (df_trades["openTime"] != "")]
            df_trades["open_time"] = pd.to_datetime(df_trades["openTime"].astype(int),unit='ms', utc=True).dt.ceil("min")

            df_trades["close_time"] = pd.to_datetime(df_trades["uTime"].astype(int),unit='ms', utc=True).dt.ceil("min")
            df_trades["ticker"] = df_trades["instId"].str.replace("-USDT-SWAP", "/USDT:USDT").replace("1000", "")
            df_trades["owner"] = uid
            df_trades.rename(columns={"lever": "leverage", "openAvgPx": "open_price", "closeAvgPx": "close_price"},
                             inplace=True)
            df_trades = df_trades[
                ["leverage", "open_price", "ticker", "open_time", "margin", "is_short", "owner", "close_time",
                 "close_price"]]
            logger.info(f"Loading in previous dataframe with {df_trades_prev.shape[0]} trades")
            logger.info(f"Current dataframe has {df_trades.shape[0]} trades")
            if df_trades_prev.shape[0] > 0:
                df_trades = df_trades_prev.combine_first(df_trades)
            logger.info(f"Merged dataframe has {df_trades.shape[0]} trades")
            df_trades.to_pickle(past_cache_fn)
            return df_trades
        else:
            raise Exception("Not a successful request at loop start")

    @cached(cache=TTLCache(maxsize=1000, ttl=clock))

    async def __okxacc_get_current_data(self, session, uid):
        current_unix = self.get_unix_timestamp()#int(datetime.utcnow().strftime("%s")) * 1000
        params = {
            'limit': '10',
            'uniqueName': uid,
            't': current_unix,
        }

        response = requests.get(
            'https://www.okx.com/priapi/v5/ecotrade/public/positions-v2',
            params=params,
            headers=self.headers_okx[0]
        )
        posdata = pd.DataFrame(response.json()["data"][0]["posData"])
        if posdata.shape[0] > 0:
            posdata["leverage"] = posdata["lever"]
            posdata["open_price"] = posdata["avgPx"]
            posdata["close_price"] = None
            posdata["close_time"] = None
            posdata["open_time"] = None




        return pd.DataFrame(columns=
                ["leverage", "open_price", "ticker", "open_time", "margin", "is_short", "owner", "close_time",
                 "close_price"])

    async def __okxacc_get_past_data(self, session, uid, get_n = 0):
        current_unix = self.get_unix_timestamp()#int(datetime.utcnow().strftime("%s")) * 1000
        params = {
            't': current_unix,
            'limit': 50,
            'uniqueName': uid,
        }
        url = 'https://www.okx.com/priapi/v5/ecotrade/public/history-positions'
        response = await session.get(
            url,
            params=params,
            headers=self.headers_okx[0]
        )
        if int(response.json()["code"]) == 0:
            df_trades = pd.DataFrame(response.json()["data"])
            df_trades.set_index('posId', inplace=True)
            current_length = df_trades.shape[0]
            past_length = 0
            error_count = 0
            got = 0
            while current_length > past_length and get_n > got:  # TODO will bug if trade_count:::200
                last_result_id = df_trades.index.astype(int).min()
                params = {
                    't': current_unix,
                    'limit': 50,
                    'uniqueName': uid,
                    "after": last_result_id
                }
                response = await session.get(url, params=params, timeout=30, headers=self.headers_okx[0])
                if response.status_code == 200 and int(response.json()["code"]) == 0:
                    testdf = pd.DataFrame(response.json()["data"])
                    past_length = current_length
                    if testdf.shape[0] > 0:
                        df_trades = df_trades.combine_first(testdf.set_index("posId"))
                        current_length = df_trades.shape[0]
                    got += 1
                else:
                    error_count += 1
                    if error_count >= 5:
                        print("Too many errors occurred while acquiring past data")
                        raise Exception("Too many errors")
            df_trades["is_short"] = False
            df_trades.loc[df_trades["posSide"] == "short", "is_short"] = True
            df_trades["open_time"] = df_trades["cTime"].apply(self.conv1s).dt.ceil("min")
            df_trades["close_time"] = df_trades["uTime"].apply(self.conv1s).dt.ceil("min")
            df_trades["ticker"] = df_trades["instId"].str.replace("-USDT-SWAP", "/USDT:USDT").replace("-USD-SWAP", "/USDT:USDT").replace("1000", "")
            df_trades["owner"] = uid
            df_trades.rename(columns={"lever": "leverage", "openAvgPx": "open_price", "closeAvgPx": "close_price"},
                             inplace=True)
            df_trades["margin"] = 0
            df_trades = df_trades[
                ["leverage", "open_price", "ticker", "open_time", "margin", "is_short", "owner", "close_time",
                 "close_price"]]
            return df_trades

        else:
            raise Exception("not correct response code for okex")

    @cached(cache=TTLCache(maxsize=1000, ttl=clock))

    async def __okx_get_current_data(self, session, uid):
        current_unix = self.get_unix_timestamp()#int(datetime.utcnow().strftime("%s")) * 1000
        params = {"t": current_unix, "uniqueName": uid}
        url_okx = "https://www.okx.com/priapi/v5/ecotrade/public/position-detail"
        response = await session.get(url_okx, params=params, timeout=30, headers=self.headers_okx[0])
        if response.status_code == 200 and int(response.json()["code"]) == 0:
            df_trades = pd.DataFrame(response.json()["data"])
            if df_trades.shape[0] > 0:
                df_trades.set_index("tradeItemId", inplace=True)
                df_trades["is_short"] = False
                df_trades.loc[((df_trades["posSide"] == "short") & (df_trades["availSubPos"].astype(float) > 0)), "is_short"] = True
                df_trades.loc[((df_trades["posSide"] == "net") & (df_trades["availSubPos"].astype(float) < 0)), "is_short"] = True
                df_trades["open_time"] = pd.to_datetime(df_trades["openTime"].astype(int),unit='ms', utc=True).dt.ceil("min")
                df_trades["ticker"] = df_trades["instId"].str.replace("-USDT-SWAP", "/USDT:USDT").replace("1000", "")
                df_trades.rename(columns={"lever": "leverage", "openAvgPx": "open_price"}, inplace=True)
                df_trades = df_trades[["leverage", "open_price", "ticker", "open_time", "margin", "is_short"]]
                df_trades["owner"] = uid
                df_trades["close_time"] = None
                df_trades["close_price"] = None
            else:
                df_trades = pd.DataFrame(
                    columns=["tradeItemId", "leverage", "open_price", "ticker", "open_time", "margin", "is_short", "owner"])
                df_trades.set_index("tradeItemId", inplace=True)
            return df_trades
        else:
            raise Exception("No proper response from okx", response.status_code)
        
    async def __feather_get_past_data(self, session, uid):
        file_name = uid if uid.endswith(".feather") else uid+".feather"
        file_name = os.path.join(os.path.dirname(os.path.realpath(__file__)), "feather", file_name)

        trades = pd.read_feather(file_name)

        if "leverage" not in trades.columns:
            trades['leverage'] = 1
        if "margin" not in trades.columns:
            trades['margin'] = 1000
        if "open_price" not in trades.columns:
            trades['open_price'] = 1
        if "open_time" not in trades.columns: # TODO separate at some point
            trades.rename(columns={"entry_time": "open_time", "exit_time": "close_time"}, inplace=True)
        if trades["open_time"].dt.tz is None:
            trades['open_time'] = pd.to_datetime(trades['open_time']).dt.tz_localize('UTC')
            trades['close_time'] = pd.to_datetime(trades['close_time']).dt.tz_localize('UTC')
        if "ticker_name" in trades.columns:
            trades.rename(columns={"ticker_name": "ticker"}, inplace=True)
        trades = trades.sort_values(by="open_time")
        trades["owner"] = uid
        trades['tradeItemId'] = trades['owner'] + trades['open_time'].astype(str) + trades['ticker']
        trades['open_time'] = trades['open_time'].dt.ceil('min')
        trades['close_time'] = trades['close_time'].dt.ceil('min')



        trades.set_index("tradeItemId", inplace=True)
        return trades

    async def __feather_get_current_data(self, session, uid):
        return self.generate_dummy(uid)

    async def __hl_get_current_data(self, session, uid):
        if (not hasattr(self, "hl_cache")) or uid not in self.hl_cache:
            past_trades, current_trades = await get_hyperliquid_trades(uid, session)


        else:
            # _, current_trades_updated = await update_hyperliquid_trades(uid, self.hl_cache[uid], session)
            past_trades, current_trades = await get_hyperliquid_trades(uid, session)
            # print(current_trades_default)
            # print(current_trades_updated)
            # if set(current_trades_updated.columns) == set(current_trades_default.columns):
            #     diff = (compare_dataframes(current_trades_default, current_trades_updated))
            #     if diff:
            #         print("WARNING DIFFERENCE")
            #         print(diff)
            # current_trades = current_trades
        if current_trades.shape[0] == 0:
            return self.generate_dummy(uid)

        # self.hl_cache[uid] = current_trades.copy()
        current_trades['owner'] = uid
        current_trades['open_time'] = current_trades['open_time'].dt.ceil("min")


        return current_trades

    async def __hl_get_past_data(self, session, uid):
        past_trades, current_trades = await get_hyperliquid_trades(uid, session)
        # if not hasattr(self, "hl_cache"):
        #     self.hl_cache = {}
        # self.hl_cache[uid] = current_trades
        if past_trades.shape[0] == 0:
            return self.generate_dummy(uid)
        past_trades['owner'] = uid
        past_trades['open_time'] = past_trades['open_time'].dt.ceil("min")
        past_trades['close_time'] = past_trades['close_time'].dt.ceil("min")
        return past_trades



    async def compile_one_current(self, platform, uid, suppress_errors=False):
        print("all methods", [method for method in vars(self) if callable(
            getattr(self, method))])
        async with httpx.AsyncClient() as session:
            function = getattr(self, f"_OpenBookBase__{platform}_get_current_data")
            if platform == "bybit" and suppress_errors:
                position_data = await function(session, uid, suppress_errors)
            else:
                position_data = await function(session, uid)
            return position_data
        
    # async def compile_one_common(self, platform, uid):
    #     past_trades, _ = await get_hyperliquid_trades(uid, session)
    #
    # async def __hl_get_all_data(self,session,uid):
    #     pass

    async def compile_one_past(self, platform, uid, get_n=0, suppress_errors=False):

        async with httpx.AsyncClient() as session:
            function = getattr(self, f"_OpenBookBase__{platform}_get_past_data")
            if platform == "bybit" and suppress_errors:
                position_data = await function(session, uid, suppress_errors)
            else:
                position_data = await function(session, uid)
            return position_data

    # async def compile_one_past_sem(self, platform, uid, get_n=0):
    #     async with httpx.AsyncClient() as session:
    #         function = getattr(self, f"_OpenBookBase__{platform}_get_past_data_async")
    #         position_data = await function(session, uid, get_n)
    #         return position_data

    async def live_compile_one(self, platform, uid, return_last=False, get_n_past=0, plimit = None, traders  = None):
        if plimit == None:
            plimit = []
        if traders == None:
            traders = pd.DataFrame()
        if return_last:
            df = (await self.convert_tradedict(await self.compile_one_current(platform, uid)))
            return {i: df_trades.iloc[-1] for i, df_trades in df.items()}
        else:# TODO NOT USED BUT AT SOME POINT MAY INDEED STINK VERY HARD
            tasks = [asyncio.create_task(self.compile_one_current(platform, uid)),
                     asyncio.create_task(self.compile_one_past(platform, uid, get_n_past))]
            # Wait for all the tasks to complete
            df = (await asyncio.gather(*tasks))
            df = df[0].combine_first(df[1])
            df = (await self.convert_tradedict(df, traders["uid"].unique().tolist(), plimit))
            return df


    # @profile
    async def live_compile_all_distributed(self, return_last=False, do_convert_tradedict=True, get_n_past=0, suppress_errors=False,
                               reverse=False, multiply_leverage=False, plimit=None, traders = None):
        df_trades = pd.DataFrame()
        if plimit is None:
            plimit = []
        if traders is None:
            traders = pd.DataFrame()

        logger.info(f"Tasks are being collected.")
        tasks = []
        semaphore = asyncio.Semaphore(self.semaphore_count)  # Limit to 10 concurrent tasks
        async def bounded_task(task):
            async with semaphore:
                return await task
        if return_last:
            
            tasks = [asyncio.create_task(bounded_task(self.compile_one_current(trader["type"], trader["uid"]))) for i, trader in
                     traders.iterrows()]
        else:
            for i, trader in traders.iterrows():
                tasks.append(asyncio.create_task(bounded_task(
                    self.compile_one_current(trader["type"], trader["uid"], suppress_errors=suppress_errors))))
                tasks.append(asyncio.create_task(bounded_task(
                    self.compile_one_past(trader["type"], trader["uid"], get_n_past, suppress_errors=suppress_errors))))
        logger.info(f"Assembled all tasks. Will begin execution with {self.semaphore_count} semaphores")
        for task in asyntq.tqdm.as_completed(tasks):
            dfr = await task
            if dfr is not None:
                df_trades = df_trades.combine_first(dfr)
        logger.info("All tasks complete")
        return df_trades

    # async def live_compile_all(self, return_last=False, do_convert_tradedict=True, get_n_past=0, suppress_errors=False,
    #                            reverse=False, multiply_leverage=False, plimit=None, traders = None):
    #     df_trades = pd.DataFrame()
    #     if plimit is None:
    #         plimit = []
    #     if traders is None:
    #         traders = pd.DataFrame()
    #
    #     logger.info("Tasks are being collected.")
    #     tasks = []
    #     if return_last:
    #         tasks = [asyncio.create_task(self.compile_one_current(trader["type"], trader["uid"])) for i, trader in
    #                  traders.iterrows()]
    #     else:
    #         for i, trader in traders.iterrows():
    #             tasks.append(asyncio.create_task(
    #                 self.compile_one_current(trader["type"], trader["uid"], suppress_errors=suppress_errors)))
    #             tasks.append(asyncio.create_task(
    #                 self.compile_one_past(trader["type"], trader["uid"], get_n_past, suppress_errors=suppress_errors)))
    #     logger.info("Assembled all tasks.")
    #     for task in asyntq.tqdm.as_completed(tasks):
    #         dfr = await task
    #         if dfr is not None:
    #             df_trades = df_trades.combine_first(dfr)
    #     logger.info("All tasks complete")
    #     if plimit is not None:
    #         df_trades["ticker"] = df_trades["ticker"].apply(lambda x: self.generate_ticker_relation(x))
    #         logger.info("Ticker relations have been generated")
    #     else:
    #         logger.info("Ticker relations have been skipped")
    #     for col in ["leverage", "open_price", "close_price", "open_price", "margin"]:
    #         try:
    #             df_trades[col] = df_trades[col].astype(float)
    #         except Exception as e:
    #             print("Error while converting types in the live_compile_all", e)
    #     logger.info("Price columns have been converted")
    #     for i, row in df_trades.iterrows():
    #         df_trades.loc[i, "owner"] = traders.loc[traders["uid"] == row["owner"], "tagname"].values[0]
    #     logger.info("Owner columns have been rendered")
    #     if reverse:
    #         df_trades["is_short"] = df_trades["is_short"].apply(lambda x: not x)
    #         logger.warning("TRADE DIRECTIONS HAVE BEEN REVERSED")
    #     if do_convert_tradedict:
    #         df_trades = self.tradedictionize(df_trades, traders["tagname"].unique().tolist(), plimit, multiply_leverage=multiply_leverage)
    #         logger.info("Tradedictionization has been completed.")
    #     return df_trades

@ray.remote
class OpenBookRemote(OpenBookBase):
    def __init__(self, *args, **kwargs):
        # The @ray.remote decorator handles the creation in a worker process.
        # We just need to ensure the base class logic is initialized.
        # print(f"[Remote Class Wrapper] Calling Base __init__") # Optional debug print
        super().__init__(*args, **kwargs)
    



class OpenBook:
    def __init__(self, trader_list=None, pairlist_limiter:list=None,cache_directory="./my_data/data/caches", flaresolverr_path = FLARESOLVERR_PATH, flaresolverr_container="tester", process_subname="obremote", namespace="orange", semaphore_compiler=4):

        self.namespace = namespace
        self.semaphore_count = semaphore_compiler



        self.trader_list = trader_list
        print(pairlist_limiter, cache_directory, flaresolverr_path, flaresolverr_container, self.semaphore_count)
        self.subname = process_subname
        if self.subname is None:

            self.remote = OpenBookBase(pairlist_limiter, cache_directory, flaresolverr_path, flaresolverr_container, self.semaphore_count)
        else:
            os.system("ray start --head --port 6200")
            ray.init(address="auto", namespace=self.namespace, ignore_reinit_error=True)
            self.remote = OpenBookRemote.options(max_concurrency=1, name=process_subname, get_if_exists=True, lifetime="detached").remote(
                pairlist_limiter, cache_directory, flaresolverr_path, flaresolverr_container, self.semaphore_count)

        logger.info("Connected/initialized a remote agent OpenBookRemote")
        self.ticker_relations = get_futures_tickers()
        if pairlist_limiter is not None:
            self.pairlist = pairlist_limiter
            self.set_pair_sources(pairlist_limiter)
            logger.info("Extended the remote pairlist")
        else:
            self.pairlist = self.ticker_relations
            self.set_pair_sources(self.ticker_relations)
            logger.info(f"Extended the remote pairlist with all {len(self.ticker_relations)} available tickers on bybit")
        self.cache_dir = cache_directory
        self.flaresolverr_path = flaresolverr_path
        self.flaresolverr_container = flaresolverr_container

        logger.info(f"Bybit has {len(self.ticker_relations)} tickers available")

    def recollect_ticker_relations(self):
        length_before = len(self.ticker_relations)
        self.ticker_relations = get_futures_tickers()
        self.pairlist = self.ticker_relations
        self.set_pair_sources(self.ticker_relations)
        length_after = len(self.ticker_relations)
        logger.info(f"Recollected the ticker relations. Changed from {length_before} to {length_after} ")


    def generate_ticker_relation(self, key):
        whitelist = self.ticker_relations
        coin_key = key.split("/")[0].strip("1000000").strip("100000").strip("10000").strip("1000").strip("k")
        for pair in whitelist:
            coin_exchange = pair.split("/")[0].strip("1000000").strip("100000").strip("10000").strip("1000").strip("k")
            if coin_exchange == coin_key:
                return pair
        return key
    
    def tradedictionize(self, dataframe, predefined_traders=None, predefined_tickers=None, start_since=0, multiply_leverage=False):
        # Handle default parameters
        if predefined_tickers is None:
            predefined_tickers = []
        if predefined_traders is None:
            predefined_traders = []
        
        # Current date, ceiled to the minute
        date_now = pd.to_datetime(datetime.utcnow(), utc=True).ceil("min")
        
        # Convert Pandas DataFrame to Polars
        df_pl = pl.from_pandas(dataframe)
        
        # Get unique tickers and traders, including predefined ones, removing duplicates
        tickers = [t for t in df_pl["ticker"].unique().to_list() if t in predefined_tickers]#list(dict.fromkeys(df_pl["ticker"].unique().to_list() + predefined_tickers)) lock in
        # tickers = [ticker for ticker in df_pl["ticker"].unique().to_list() if ticker in predefined_tickers]
        traders = list(dict.fromkeys(df_pl["owner"].unique().to_list() + predefined_traders))
        
        # Define all expected columns
        base_columns = ["short_position_count", "long_position_count", "long_trader_count", "short_trader_count", "owner"]
        owner_columns = [f"{prefix}_{owner}" for owner in traders for prefix in ["short", "short_margin", "long", "long_margin"]]
        all_columns = base_columns + owner_columns
        
        tickers_dict = {}
        for ticker in tickers:
            # Filter trades for the current ticker
            dft_pl = df_pl.filter(pl.col("ticker") == ticker)
            
            # Case 1: No trades for this ticker
            if dft_pl.is_empty():
                df = pl.DataFrame({"date": [date_now]})
                # Initialize all columns with zeros or empty strings
                for col in all_columns:
                    if col == "owner":
                        df = df.with_columns(pl.lit("").alias(col))
                    else:
                        df = df.with_columns(pl.lit(0).cast(pl.Float64).alias(col))
                tickers_dict[ticker] = df.to_pandas().set_index("date")
                continue
            
            # Case 2: Process trades
            # Determine start date
            if start_since == 0:
                start_date = dft_pl["open_time"].min() - timedelta(minutes=1)
            else:
                start_date = date_now - timedelta(days=start_since)
            date_range = pl.datetime_range(start_date, date_now, interval=timedelta(minutes=1), eager=True)
            base_df = pl.DataFrame({"date": date_range})
            
            # Handle open trades (close_time is NaT/None)
            dft_pl = dft_pl.with_columns(
                pl.when(pl.col("close_time").is_null())
                .then(date_now)
                .otherwise(pl.col("close_time"))
                .alias("effective_close_time")
            )
            
            # Generate trade data over time
            trade_dfs = []
            for row in dft_pl.iter_rows(named=True):
                open_time = row["open_time"]
                close_time = row["effective_close_time"]
                trade_dates = pl.datetime_range(open_time, close_time, interval=timedelta(minutes=1), eager=True)
                leverage_mult = float(row["leverage"]) if multiply_leverage else 1.0
                margin = float(row["margin"]) * leverage_mult  # relative_margin assumed 1
                trade_df = pl.DataFrame({
                    "date": trade_dates,
                    "owner": row["owner"],
                    "is_short": row["is_short"],
                    "margin": margin
                })
                trade_dfs.append(trade_df)
            all_trades_df = pl.concat(trade_dfs, how="vertical")
            
            # Aggregate per date, owner, and position type
            agg_df = all_trades_df.group_by(["date", "owner", "is_short"]).agg([
                pl.count("margin").alias("position_count"),
                pl.sum("margin").alias("total_margin")
            ])
            
            # Pivot for short positions
            # Pivot for short positions
            short_df = agg_df.filter(pl.col("is_short")).pivot(
                index="date",
                columns="owner",
                values=["position_count", "total_margin"],
                aggregate_function="sum"
            )

            # Create rename mapping for short_df
            short_rename_mapping = {}
            prefix_pos = "position_count_"
            prefix_margin = "total_margin_"
            len_pos = len(prefix_pos)
            len_margin = len(prefix_margin)

            # Use list(short_df.columns) to avoid modifying during iteration if needed, though simple iteration is often fine
            for col_name in short_df.columns:
                 if col_name == "date":  # Skip the index column
                     continue
                 if col_name.startswith(prefix_pos):
                     actual_owner = col_name[len_pos:] # Extract the original owner name
                     short_rename_mapping[col_name] = f"short_{actual_owner}"
                 elif col_name.startswith(prefix_margin):
                     actual_owner = col_name[len_margin:] # Extract the original owner name
                     short_rename_mapping[col_name] = f"short_margin_{actual_owner}"

            # Apply renaming if the mapping is not empty
            if short_rename_mapping:
                short_df = short_df.rename(short_rename_mapping)
            
            # Pivot for long positions
            # Pivot for long positions
            long_df = agg_df.filter(~pl.col("is_short")).pivot(
                index="date",
                columns="owner",
                values=["position_count", "total_margin"],
                aggregate_function="sum"
            )

            # Create rename mapping for long_df (using the same prefixes)
            long_rename_mapping = {}
            # Prefixes defined above are reused here
            # len_pos and len_margin defined above are reused here

            for col_name in long_df.columns:
                 if col_name == "date": # Skip the index column
                     continue
                 if col_name.startswith(prefix_pos):
                     actual_owner = col_name[len_pos:] # Extract the original owner name
                     long_rename_mapping[col_name] = f"long_{actual_owner}"
                 elif col_name.startswith(prefix_margin):
                     actual_owner = col_name[len_margin:] # Extract the original owner name
                     long_rename_mapping[col_name] = f"long_margin_{actual_owner}"

            # Apply renaming if the mapping is not empty
            if long_rename_mapping:
                long_df = long_df.rename(long_rename_mapping)
            
            
            # Add missing owner-specific columns
            for owner in traders:
                for col in [f"short_{owner}", f"short_margin_{owner}"]:
                    if col not in short_df.columns:
                        short_df = short_df.with_columns(pl.lit(0.0).alias(col))
                for col in [f"long_{owner}", f"long_margin_{owner}"]:
                    if col not in long_df.columns:
                        long_df = long_df.with_columns(pl.lit(0.0).alias(col))
            
            # Compute totals
            total_agg_df = all_trades_df.group_by(["date", "is_short"]).agg([
                pl.count("owner").alias("position_count"),
                pl.col("owner").n_unique().alias("trader_count"),
                pl.sum("margin").alias("total_margin")
            ])
            short_total_df = total_agg_df.filter(pl.col("is_short")).select([
                "date",
                pl.col("position_count").alias("short_position_count"),
                pl.col("trader_count").alias("short_trader_count"),
                pl.col("total_margin").alias("short_margin")
            ])
            long_total_df = total_agg_df.filter(~pl.col("is_short")).select([
                "date",
                pl.col("position_count").alias("long_position_count"),
                pl.col("trader_count").alias("long_trader_count"),
                pl.col("total_margin").alias("long_margin")
            ])
            
            # Compute owner string column
            owner_df = all_trades_df.group_by("date").agg(
                pl.col("owner").alias("owners")
            ).with_columns(
                pl.col("owners").list.join(";").alias("owner")
            ).select(["date", "owner"])
            
            # Combine all data
            df = (base_df
                .join(short_df, on="date", how="left")
                .join(long_df, on="date", how="left")
                .join(short_total_df, on="date", how="left")
                .join(long_total_df, on="date", how="left")
                .join(owner_df, on="date", how="left"))
            
            # Fill missing values
            numeric_cols = [col for col in df.columns if col != "date" and col != "owner"]
            df = df.with_columns([pl.col(col).fill_null(0.0) for col in numeric_cols])
            df = df.with_columns(pl.col("owner").fill_null(""))
            
            # Ensure all columns are present
            for col in all_columns:
                if col not in df.columns:
                    dtype = pl.Utf8 if col == "owner" else pl.Float64
                    df = df.with_columns(pl.lit(0 if dtype != pl.Utf8 else "").cast(dtype).alias(col))
            
            # Convert to Pandas DataFrame with date index
            tickers_dict[ticker] = df.to_pandas().set_index("date")
        
        return tickers_dict

    def tradedictionize_pandas(self, dataframe, predefined_traders=None, predefined_tickers=None, start_since=-1, multiply_leverage=False):
        if predefined_tickers is None:
            predefined_tickers = []
        if predefined_traders is None:
            predefined_traders = []
        date_now = pd.to_datetime(datetime.utcnow(), utc=True).ceil("min")
        # tickers_to_gen = list(dict.fromkeys(dataframe["ticker"].unique().tolist() + predefined_tickers))
        tickers_in_data = dataframe["ticker"].unique().tolist()
        traders_in_data = dataframe["owner"].unique().tolist()
        tickers_to_gen = [ticker for ticker in tickers_in_data if ticker in predefined_tickers]
        traders_to_gen = list(dict.fromkeys(traders_in_data + predefined_traders))

        tickers_dict = {}
        for ticker in tickers_to_gen:
            dft = dataframe[dataframe["ticker"] == ticker]
            
            # Define all columns
            base_columns = ["short_position_count", "long_position_count", "long_trader_count", "short_trader_count", "owner"]
            owner_columns = [f"{prefix}_{owner}" for owner in traders_to_gen for prefix in ["short", "short_margin", "long", "long_margin"]]
            all_columns = base_columns + owner_columns

            if dft.shape[0] == 0:
                df = pd.DataFrame(index=[date_now], columns=all_columns)
                df.index.name = "date"
                df[base_columns[:-1]] = 0
                df["owner"] = ""
                df[owner_columns] = 0.0
                tickers_dict[ticker] = df
                continue

            if start_since == -1:
                start_date = dft["open_time"].min() - timedelta(minutes=1)
            else:
                start_date = date_now - timedelta(minutes=start_since)

            dataflen = pd.date_range(start_date, date_now, freq="min")
            df = pd.DataFrame(index=dataflen, columns=all_columns)
            df.index.name = "date"
            df[base_columns[:-1]] = 0
            df["owner"] = ""
            df[owner_columns] = 0.0

            for i, trade in dft.iterrows():
                owner = trade["owner"]
                is_short = trade["is_short"]
                open_time = trade["open_time"] if not pd.isna(trade["open_time"]) else None
                close_time = trade["close_time"] if not pd.isna(trade["close_time"]) else None

                if not pd.isna(close_time):
                    df.loc[open_time:close_time, "owner"] = df.loc[open_time:close_time, "owner"] + f"{owner};"
                else:
                    df.loc[open_time:, "owner"] = df.loc[open_time:, "owner"] + f"{owner};"

                relative_margin = 1  # Replace with your margin logic
                leverage_mult = trade["leverage"] if multiply_leverage else 1

                if trade["is_short"]:
                    df.loc[open_time:close_time, f"short_{owner}"] += 1
                    df.loc[open_time:close_time, f"short_position_count"] += 1
                    df.loc[open_time:close_time, f"short_margin_{owner}"] += float(trade["margin"]) * float(leverage_mult) / float(relative_margin)
                else:
                    df.loc[open_time:close_time, f"long_{owner}"] += 1
                    df.loc[open_time:close_time, f"long_position_count"] += 1
                    df.loc[open_time:close_time, f"long_margin_{owner}"] += float(trade["margin"]) * float(leverage_mult) / float(relative_margin)

            columns_short = [f"short_{owner}" for owner in traders_to_gen]
            shortmrg = [f"short_margin_{owner}" for owner in traders_to_gen]
            df["short_trader_count"] = (df[columns_short] > 0).sum(axis=1)
            columns_long = [f"long_{owner}" for owner in traders_to_gen]
            longmrg = [f"long_margin_{owner}" for owner in traders_to_gen]
            df["long_trader_count"] = (df[columns_long] > 0).sum(axis=1)
            df["short_margin"] = df[shortmrg].sum(axis=1)
            df["long_margin"] = df[longmrg].sum(axis=1)

            tickers_dict[ticker] = df

        return tickers_dict


    def tradedictionize_efficient(self, dataframe, predefined_traders=None, predefined_tickers=None, start_since=0, multiply_leverage=False):
        """
        Memory-efficient version of tradedictionize using an event-based approach.
        v 3: Fixes issue where the last timestamp showed zero values due to
                   handling of open trades' effective close time.
        """
        if predefined_tickers is None:
            predefined_tickers = []
        if predefined_traders is None:
            predefined_traders = []
        dataframe['open_time'] = pd.to_datetime(dataframe['open_time'], utc=True)
        dataframe['close_time'] = pd.to_datetime(dataframe['close_time'], utc=True)
        dataframe['margin'] = pd.to_numeric(dataframe['margin'], errors='coerce').fillna(0.0)
        dataframe['leverage'] = pd.to_numeric(dataframe['leverage'], errors='coerce').fillna(1.0)
        dataframe['is_short'] = dataframe['is_short'].astype(bool)

        date_now = pd.to_datetime(datetime.utcnow(), utc=True).ceil("min")

        tickers_in_data = dataframe["ticker"].unique().tolist()
        traders_in_data = dataframe["owner"].unique().tolist()
        all_tickers = [ticker for ticker in tickers_in_data if ticker in predefined_tickers]
        all_traders = sorted(list(dict.fromkeys(predefined_traders + traders_in_data)))

        tickers_dict = {}
        logger.info(f"Starting efficient tradedictionize V3 for {len(all_tickers)} tickers and {len(all_traders)} traders.")

        for ticker in all_tickers:
            dft = dataframe[dataframe["ticker"] == ticker].copy()

            
            col_short_pos_count = "short_position_count"
            col_long_pos_count = "long_position_count"
            col_short_trader_count = "short_trader_count"
            col_long_trader_count = "long_trader_count"
            col_short_margin_total = "short_margin"
            col_long_margin_total = "long_margin"
            col_owner = "owner"

            owner_short_pos_cols = {owner: f"short_{owner}" for owner in all_traders}
            owner_long_pos_cols = {owner: f"long_{owner}" for owner in all_traders}
            owner_short_margin_cols = {owner: f"short_margin_{owner}" for owner in all_traders}
            owner_long_margin_cols = {owner: f"long_margin_{owner}" for owner in all_traders}

            all_numeric_cols_base = [col_short_pos_count, col_long_pos_count,
                                     col_short_trader_count, col_long_trader_count,
                                     col_short_margin_total, col_long_margin_total]
            all_owner_numeric_cols = list(owner_short_pos_cols.values()) + list(owner_long_pos_cols.values()) + \
                                     list(owner_short_margin_cols.values()) + list(owner_long_margin_cols.values())
            all_numeric_cols = all_numeric_cols_base + all_owner_numeric_cols
            all_cols = all_numeric_cols + [col_owner] # Maintain final desired order

            
            if dft.empty:
                empty_data = {col: 0.0 for col in all_numeric_cols}
                empty_data[col_owner] = ""
                final_df = pd.DataFrame(empty_data, index=pd.DatetimeIndex([date_now], name="date"))
                final_df = final_df[all_cols]
                tickers_dict[ticker] = final_df
                continue

            
            # *** THE FIX IS HERE ***
            # For open trades (close_time is NaT), set the effective close time *after*
            # the final timestamp we are calculating for (date_now). This ensures their
            # state is correctly forward-filled into the 'date_now' row by reindex/ffill,
            # as the negative 'close' event occurs after our period of interest.
            effective_end_time = date_now + timedelta(minutes=1)
            dft['effective_close_time'] = dft['close_time'].fillna(effective_end_time)
            # *** END FIX ***

            dft['margin_adjusted'] = dft['margin'] * (dft['leverage'] if multiply_leverage else 1.0)

            min_ts = dft['open_time'].min()
            if start_since > 0:
                calc_start_date = date_now - timedelta(days=start_since)
                start_date = max(min_ts - timedelta(minutes=1), calc_start_date)
            else:
                start_date = min_ts - timedelta(minutes=1)
            start_date = min(start_date, date_now - timedelta(minutes=1))
            full_date_range = pd.date_range(start=start_date, end=date_now, freq='min', name='date')

            
            open_events = dft[['open_time', 'owner', 'is_short', 'margin_adjusted']].copy()
            open_events.rename(columns={'open_time': 'timestamp'}, inplace=True)
            open_events['pos_change'] = 1
            open_events['margin_change'] = open_events['margin_adjusted']

            # Use the potentially adjusted effective_close_time here
            close_events = dft[['effective_close_time', 'owner', 'is_short', 'margin_adjusted']].copy()
            close_events.rename(columns={'effective_close_time': 'timestamp'}, inplace=True)
            close_events['pos_change'] = -1
            close_events['margin_change'] = -close_events['margin_adjusted']

            events = pd.concat([open_events, close_events], ignore_index=True)
            events = events.drop(columns=['margin_adjusted'])
            # Filter events >= start_date. Note: events at effective_end_time (date_now + 1min)
            # might be included here but won't affect the final result up to date_now
            # because reindex stops at date_now.
            events = events[events['timestamp'] >= start_date]

            
            agg_changes = events.groupby(['timestamp', 'owner', 'is_short']).sum(numeric_only=True).reset_index()

            
            pos_pivot = agg_changes.pivot_table(index='timestamp', columns=['owner', 'is_short'], values='pos_change', fill_value=0)
            margin_pivot = agg_changes.pivot_table(index='timestamp', columns=['owner', 'is_short'], values='margin_change', fill_value=0)

            
            def flatten_cols(df, suffix):
                if isinstance(df.columns, pd.MultiIndex):
                    new_cols = []
                    for owner, is_short in df.columns:
                        side = "short" if is_short else "long"
                        # Ensure owner names with special chars/underscores are handled if necessary
                        new_cols.append(f"{side}_{suffix}_{owner}")
                    df.columns = new_cols
                return df

            pos_pivot = flatten_cols(pos_pivot, 'pos')
            margin_pivot = flatten_cols(margin_pivot, 'margin')

            # Combine position and margin changes using transpose method for groupby
            all_changes_unagg = pd.concat([pos_pivot, margin_pivot], axis=1)
            all_changes = all_changes_unagg.T.groupby(level=0).sum().T


            
            expected_change_cols = [f"{side}_pos_{owner}" for owner in all_traders for side in ['short', 'long']] + \
                                   [f"{side}_margin_{owner}" for owner in all_traders for side in ['short', 'long']]
            all_changes = all_changes.reindex(columns=expected_change_cols, fill_value=0)

            state_at_events = all_changes.cumsum()

            final_col_mapping = {}
            for col in state_at_events.columns:
                parts = col.split('_') # Assumes owner names don't have underscores matching _pos_ or _margin_
                side, type, owner = parts[0], parts[1], '_'.join(parts[2:])
                if type == 'pos':
                    final_col_mapping[col] = owner_short_pos_cols.get(owner) if side == 'short' else owner_long_pos_cols.get(owner)
                elif type == 'margin':
                    final_col_mapping[col] = owner_short_margin_cols.get(owner) if side == 'short' else owner_long_margin_cols.get(owner)
                # Filter out None values if an owner from events is somehow not in all_traders (shouldn't happen)
            final_col_mapping = {k: v for k, v in final_col_mapping.items() if v is not None}
            state_at_events.rename(columns=final_col_mapping, inplace=True)


            
            # Reindex up to date_now. ffill will use the state from the last event <= index time.
            # Since close events for open trades are now *after* date_now, the state at date_now
            # will correctly reflect them being open.
            final_df = state_at_events.reindex(full_date_range, method='ffill').fillna(0)

            
            active_owner_short_pos_cols = [col for col in owner_short_pos_cols.values() if col in final_df.columns]
            active_owner_long_pos_cols = [col for col in owner_long_pos_cols.values() if col in final_df.columns]
            active_owner_short_margin_cols = [col for col in owner_short_margin_cols.values() if col in final_df.columns]
            active_owner_long_margin_cols = [col for col in owner_long_margin_cols.values() if col in final_df.columns]

            new_agg_cols = {}
            new_agg_cols[col_short_pos_count] = final_df[active_owner_short_pos_cols].sum(axis=1)
            new_agg_cols[col_long_pos_count] = final_df[active_owner_long_pos_cols].sum(axis=1)
            new_agg_cols[col_short_trader_count] = (final_df[active_owner_short_pos_cols] > 0).sum(axis=1)
            new_agg_cols[col_long_trader_count] = (final_df[active_owner_long_pos_cols] > 0).sum(axis=1)
            new_agg_cols[col_short_margin_total] = final_df[active_owner_short_margin_cols].sum(axis=1)
            new_agg_cols[col_long_margin_total] = final_df[active_owner_long_margin_cols].sum(axis=1)

            
            active_owners_series = pd.Series("", index=final_df.index, dtype=str)
            owner_cols_present = [c for c in list(owner_short_pos_cols.values()) + list(owner_long_pos_cols.values()) if c in final_df.columns]
            if owner_cols_present:
                active_mask = final_df[owner_cols_present] > 0
                owner_map = {col: '_'.join(col.split('_')[1:]) for col in owner_cols_present}
                active_owners_per_row = [sorted(list(set(owner_map[col] for col in active_mask.columns[row_mask]))) for row_mask in active_mask.values]
                active_owners_series = pd.Series([';'.join(owners) for owners in active_owners_per_row], index=final_df.index)

            new_agg_cols[col_owner] = active_owners_series

            # Assign all new aggregate columns at once
            final_df = final_df.assign(**new_agg_cols)

            
            final_df = final_df.reindex(columns=all_cols, fill_value=None)

            fill_values = {col: 0.0 for col in all_numeric_cols}
            fill_values[col_owner] = ""
            final_df.fillna(value=fill_values, inplace=True)

            for col in all_numeric_cols:
                if col in final_df.columns:
                    final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0.0)
            final_df[col_owner] = final_df[col_owner].astype(str)


            tickers_dict[ticker] = final_df.copy()

        logger.info("Finished efficient tradedictionize V3.")
        return tickers_dict




    async def live_compile_one(self, platform, uid, return_last=False, get_n_past=0):
        try:
            if self.subname is None:
                return await self.remote.live_compile_one(platform, uid, return_last, get_n_past, self.pairlist, self.trader_list)
            else:
                return ray.get(self.remote.live_compile_one.remote(platform, uid, return_last, get_n_past, self.pairlist, self.trader_list))
        except ray.exceptions.ActorDiedError as e:
            logger.error(traceback.format_exc())
            self.restart_ray()
        except ray.exceptions.RaySystemError as e:
            logger.error(traceback.format_exc())
            self.restart_ray()

    async def live_compile_all(self, return_last=False, do_convert_tradedict=True, get_n_past=0,
                             suppress_errors=False, reverse=False, multiply_leverage=False, bugged=True):
        logger.info("Launched all compiler")
        try:
            if self.subname is None:
                df_trades = await self.remote.live_compile_all_distributed(return_last, do_convert_tradedict,
                                                                            get_n_past, suppress_errors, reverse, multiply_leverage, self.pairlist, self.trader_list)
            else:
                df_trades = ray.get(self.remote.live_compile_all_distributed.remote(return_last, do_convert_tradedict,
                                                                                    get_n_past, suppress_errors, reverse, multiply_leverage, self.pairlist, self.trader_list))
            logger.info("Everything has been compiled on a remote process")
            if self.pairlist is not None:
                df_trades["ticker"] = df_trades["ticker"].apply(lambda x: self.generate_ticker_relation(x))
                logger.info("Ticker relations have been generated")
            else:
                logger.info("Ticker relations have been skipped")
            for col in ["leverage", "open_price", "close_price", "open_price", "margin"]:
                try:
                    df_trades[col] = df_trades[col].astype(float)
                except Exception as e:
                    print("Error while converting types in the live_compile_all", e)
            logger.info("Price columns have been converted")
            for i, row in df_trades.iterrows():
                df_trades.loc[i, "owner"] = self.trader_list.loc[self.trader_list["uid"] == row["owner"], "tagname"].values[0]
            logger.info("Owner columns have been rendered")
            if reverse:
                df_trades["is_short"] = df_trades["is_short"].apply(lambda x: not x)
                logger.warning("TRADE DIRECTIONS HAVE BEEN REVERSED")
            if do_convert_tradedict:
                if not return_last:
                    df_trades = self.tradedictionize_efficient(df_trades, self.trader_list["tagname"].unique().tolist(), self.pairlist, multiply_leverage=multiply_leverage)
                else:
                    df_trades = self.tradedictionize_pandas(df_trades, self.trader_list["tagname"].unique().tolist(), self.pairlist, multiply_leverage=multiply_leverage, start_since=2)
                    # use inefficient version, since despite handling larger dataframes less efficiently might handle smaller dataframes more correctly.
                # compare
                # df_trades2 = self.tradedictionize(df_trades, self.trader_list["tagname"].unique().tolist(), self.pairlist, multiply_leverage=multiply_leverage)
                # for tick, df in df_trades1.items():
                #     print(compare_dataframes(df, df_trades2[tick]), "df_trades vs df_trades1 at", tick)
                # df_trades = df_trades1
                # TODO compare
                logger.info("Tradedictionization has been completed.")
                if return_last:
                    s =  {i: df.iloc[-1].copy() for i, df in df_trades.items()}
                    del df_trades
                    gc.collect()
                    return s
            return df_trades
        except ray.exceptions.ActorDiedError as e:
            logger.error(traceback.format_exc())
            self.restart_ray()
        except ray.exceptions.RaySystemError as e:
            logger.error(traceback.format_exc())
            self.restart_ray()
        except Exception as e:
            if bugged:
                logger.error(traceback.format_exc())
                self.restart_ray()
            else:
                raise Exception(e)

    async def okx_fetch_extended_day_count(self, uid):
        params = {
            # 'latestNum': '0',
            'uniqueName': uid,
            't': self.get_unix_timestamp()#str(int(datetime.now().timestamp() * 1000)),
        }
        headers_okx = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'app-type': 'web',
            'cookie': 'locale=en_US; preferLocale=en_US; ok_prefer_udColor=0; ok_prefer_udTimeZone=0; browserVersionLevel=v5.6ad2a8e37c01; devId=c79ad69f-b9d8-42ca-9148-e25d2d91674d; OptanonAlertBoxClosed=2024-04-16T17:57:13.992Z; intercom-id-ny9cf50h=36027190-7598-46c9-801e-6fe9e1a58ec8; intercom-device-id-ny9cf50h=21366273-b7fa-4de5-9b66-c75fe34345b8; amp_56bf9d=c79ad69f-b9d8-42ca-9148-e25d2d91674d...1hti0utnh.1hti0vthp.68.0.68; ok_site_info===Qf3ojI5RXa05WZiwiIMFkQPx0Rfh1SPJiOiUGZvNmIsIiVMJiOi42bpdWZyJye; first_ref=https%3A%2F%2Fwww.okx.com%2Fcopy-trading%2Faccount%2FB0D4CDE722DB850E%3Ftab%3Dswap; fingerprint_id=c79ad69f-b9d8-42ca-9148-e25d2d91674d; __cf_bm=KnXKMt8UzZkypDe1slOXK5SFPJy0vFJbugM8fRmmim4-1717177807-1.0.1.1-dcW52Nx48AyZq6mlsSDcU7KSK9ft.HOVTsWRmdYKwHrG1t1j7EHhutLOvxNi9Rvk.nZYie.GlmLWftWmG3Nxnw; OptanonConsent=isGpcEnabled=0&datestamp=Fri+May+31+2024+20%3A50%3A07+GMT%2B0300+(Eastern+European+Summer+Time)&version=202212.1.0&isIABGlobal=false&hosts=&consentId=b80b34cc-df5d-40e1-a473-3388906d1899&interactionCount=1&landingPath=NotLandingPage&groups=C0004%3A0%2CC0002%3A0%2CC0001%3A1%2CC0003%3A0&geolocation=LV%3BRIX&AwaitingReconsent=false; okg.currentMedia=sm; ok-ses-id=P91+vWHN9SWDOKVUnhl6iS33b17G4UrJVPdwshSA06Q6D6bltyw3TOk+GyiXId9aYun+5bWSHRAMUzBJf0YQds4fD/FZJnxy8J744VEw/p81SB67QVkAkif2VP195Zy3; traceId=1020171778151390003; ok_prefer_currency=%7B%22currencyId%22%3A7%2C%22isDefault%22%3A1%2C%22isPremium%22%3Afalse%2C%22isoCode%22%3A%22EUR%22%2C%22precision%22%3A2%2C%22symbol%22%3A%22%E2%82%AC%22%2C%22usdToThisRate%22%3A0.923%2C%22usdToThisRatePremium%22%3A0.923%2C%22displayName%22%3A%22EUR%22%7D; _monitor_extras={"deviceId":"E3u7Ij2A-XxDMaO9u3wRdS","eventId":727,"sequenceNumber":727}',
            'devid': 'c79ad69f-b9d8-42ca-9148-e25d2d91674d',
            'dnt': '1',
            'priority': 'u=1, i',
            'referer': 'https://www.okx.com/copy-trading/account/2BE980C9BEA40361?tab=swap',
            'sec-ch-ua': '"Chromium";v="125", "Not.A/Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'x-cdn': 'https://www.okx.com',
            'x-id-group': '1030771778070800004-c-31',
            'x-locale': 'en_US',
            'x-site-info': '==Qf3ojI5RXa05WZiwiIMFkQPx0Rfh1SPJiOiUGZvNmIsIiVMJiOi42bpdWZyJye',
            'x-utc': '3',
            'x-zkdex-env': '0',
        }

        async with httpx.AsyncClient() as session:
            # response = await session.get(
            #     'https://www.okx.com/priapi/v5/ecotrade/public/yield-pnl',
            #     params=params, headers=self.headers_okx[0]
            # )
            response = await session.get(
                'https://www.okx.com/priapi/v5/ecotrade/public/total-pnl',
                params=params, headers=headers_okx
            )
            if response.status_code != 200:
                raise Exception(f"status code is {response.status_code}")

            response = response.json()

            if response["code"] != "0":
                raise Exception(f"Response OKX status code is {response['code']}")
            print(response["data"][0]["statTime"])
            result = pl.from_dicts(response["data"])
            result = result.with_columns(
                (pl.col("ratio").ne(pl.col("ratio").shift(1))).alias("balance_changed")
            )
            days_with_changes = result.filter(pl.col("balance_changed")).height - 1
            initial_investment = 10000
            returned_graph = result.with_columns(pl.col("ratio").cast(pl.Float64).mul(initial_investment).add(initial_investment).alias("total_balance"), pl.from_epoch(pl.col("statTime").cast(pl.Int64), time_unit="ms").alias("date")).select(["date", "total_balance"])
            return days_with_changes, returned_graph

    def get_unix_timestamp(self):
        return str(int(time.time() * 1000))





    def set_pair_sources(self, p):
        if self.subname is None:
            self.remote.extend_pairlist(p)
        else:
            ray.get(self.remote.extend_pairlist.remote(p))

    def set_sources(self, traders):
        self.trader_list = traders
        if 'bybit' in self.trader_list['uid'].tolist():
            if self.subname is None:
                self.remote.__generate_bybit_headers()
            else:
                ray.get(self.remote.__generate_bybit_headers())

    def add_to_pairlist(self, pair):
        self.set_pair_sources(list(set(self.pairlist + [pair])))
        
    def remove_from_pairlist(self, pair):
        self.pairlist.remove(pair)

    def stop_ray(self):
        os.system("ray stop")
        self.restart_ray()
        
    def restart_ray(self):
        if self.subname is not None:

            logger.info("Restarting ray..")
            os.system("ray start --head --port 6200")
            ray.shutdown()

            ray.init(address="auto", namespace=self.namespace, ignore_reinit_error=True)
            self.remote = OpenBookRemote.options(max_concurrency=1, name=self.subname, get_if_exists=True,
                                                 lifetime="detached").remote(
                self.pairlist, self.cache_dir, self.flaresolverr_path, self.flaresolverr_container, self.semaphore_count)
        else:
            logger.warning(f"Nothing is done as subname is None")
        
    # def fetch_bybit_copytrading_tickers(self):
    #     params = {
    #         'filter': 'all',
    #     }
    #     headers_bybit = {
    #         "Sec-Fetch-Mode": "no-cors",
    #         "TE": "trailers",
    #         "Sec-Fetch-Site": "same-site",
    #         "Accept": "*/*",
    #         "Accept-Encoding": "gzip, deflate, br",
    #         "Accept-Language": "en-US,en;q=0.5",
    #         "Connection": "keep-alive",
    #         "Origin": "https://www.bybit.com",
    #         "Referer": "https://www.bybit.com/",
    #         "DNT": "1",
    #         "Pragma": "no-cache",
    #         "Cache-Control": "no-cache",
    #         "Sec-Fetch-Dest": "empty",
    #         "Host": "api2.bybit.com",
    #     }
    #     response = requests.get(
    #         'https://api2.bybit.com/contract/v5/product/dynamic-symbol-list',
    #         params=params,
    #         headers=headers_bybit,
    #     )
    #     if response.status_code != 200:
    #         logger.error(f"Status code is {response.status_code}")
    #         return
    #     df = pl.from_dicts(response.json()["result"]["LinearPerpetual"])
    #     df = df.filter(pl.col("supportCopyTrade") == True)
    #     symbols = df.with_columns(pl.concat_str(
    #     [
    #         pl.col("baseCurrency"),
    #         pl.lit("/"),
    #         pl.col("quoteCurrency"),
    #         pl.lit(":"),
    #         pl.col("quoteCurrency")
    #     ],
    #     separator="",
    #         ).alias("symbol"))["symbol"].to_list()
    #     return symbols

    def limit_to_copytraded(self):
        tickers = ["STG/USDT:USDT", "CTSI/USDT:USDT", "UMA/USDT:USDT", "JOE/USDT:USDT", "CRV/USDT:USDT", "FRONT/USDT:USDT", "YGG/USDT:USDT", "ALGO/USDT:USDT", "BLUR/USDT:USDT", "WIF/USDT:USDT", "LPT/USDT:USDT", "WOO/USDT:USDT", "ZRX/USDT:USDT", "MANTA/USDT:USDT", "BIGTIME/USDT:USDT", "GMT/USDT:USDT", "HIGH/USDT:USDT", "COTI/USDT:USDT", "STRK/USDT:USDT", "ETH/USDT:USDT", "ETC/USDT:USDT", "SUSHI/USDT:USDT", "RLC/USDT:USDT", "COMP/USDT:USDT", "AMB/USDT:USDT", "MAV/USDT:USDT", "PYTH/USDT:USDT", "POPCAT/USDT:USDT", "MKR/USDT:USDT", "HIFI/USDT:USDT", "TON/USDT:USDT", "ICP/USDT:USDT", "MTL/USDT:USDT", "JUP/USDT:USDT", "ALPHA/USDT:USDT", "SAND/USDT:USDT", "BNB/USDT:USDT", "AGIX/USDT:USDT", "LQTY/USDT:USDT", "1000WEN/USDT:USDT", "PEOPLE/USDT:USDT", "LTC/USDT:USDT", "KAS/USDT:USDT", "LINK/USDT:USDT", "LRC/USDT:USDT", "STPT/USDT:USDT", "AGLD/USDT:USDT", "TIA/USDT:USDT", "FIL/USDT:USDT", "STORJ/USDT:USDT", "PORTAL/USDT:USDT", "ORBS/USDT:USDT", "BSV/USDT:USDT", "NEAR/USDT:USDT", "RVN/USDT:USDT", "SOL/USDT:USDT", "ASTR/USDT:USDT", "AXS/USDT:USDT", "KSM/USDT:USDT", "AI/USDT:USDT", "XLM/USDT:USDT", "1000RATS/USDT:USDT", "ACH/USDT:USDT", "CHZ/USDT:USDT", "INJ/USDT:USDT", "ETHFI/USDT:USDT", "ACE/USDT:USDT", "EGLD/USDT:USDT", "KAVA/USDT:USDT", "UNFI/USDT:USDT", "GALA/USDT:USDT", "LOOM/USDT:USDT", "REEF/USDT:USDT", "BEL/USDT:USDT", "LDO/USDT:USDT", "POLYX/USDT:USDT", "POWR/USDT:USDT", "1INCH/USDT:USDT", "CELO/USDT:USDT", "ZETA/USDT:USDT", "KEY/USDT:USDT", "GLMR/USDT:USDT", "GLM/USDT:USDT", "ANKR/USDT:USDT", "XVG/USDT:USDT", "REN/USDT:USDT", "UNI/USDT:USDT", "ADA/USDT:USDT", "BRETT/USDT:USDT", "EDU/USDT:USDT", "IMX/USDT:USDT", "RUNE/USDT:USDT", "SPELL/USDT:USDT", "XAI/USDT:USDT", "API3/USDT:USDT", "PIXEL/USDT:USDT", "STMX/USDT:USDT", "APT/USDT:USDT", "MASK/USDT:USDT", "AEVO/USDT:USDT", "YFI/USDT:USDT", "DYDX/USDT:USDT", "ETHW/USDT:USDT", "FET/USDT:USDT", "XVS/USDT:USDT", "KLAY/USDT:USDT", "AAVE/USDT:USDT", "BAND/USDT:USDT", "PNUT/USDT:USDT", "1000LUNC/USDT:USDT", "FLOW/USDT:USDT", "OP/USDT:USDT", "MDT/USDT:USDT", "AR/USDT:USDT", "JASMY/USDT:USDT", "DYM/USDT:USDT", "MATIC/USDT:USDT", "EOS/USDT:USDT", "IOTA/USDT:USDT", "MYRO/USDT:USDT", "SNX/USDT:USDT", "GMX/USDT:USDT", "OGN/USDT:USDT", "BCH/USDT:USDT", "RNDR/USDT:USDT", "SEI/USDT:USDT", "WLD/USDT:USDT", "PENDLE/USDT:USDT", "ATOM/USDT:USDT", "ID/USDT:USDT", "GRT/USDT:USDT", "CAKE/USDT:USDT", "THETA/USDT:USDT", "BLZ/USDT:USDT", "HBAR/USDT:USDT", "LUNA2/USDT:USDT", "TRB/USDT:USDT", "OM/USDT:USDT", "MAVIA/USDT:USDT", "SLP/USDT:USDT", "WAVES/USDT:USDT", "SSV/USDT:USDT", "ALT/USDT:USDT", "TRX/USDT:USDT", "CFX/USDT:USDT", "ARPA/USDT:USDT", "1000FLOKI/USDT:USDT", "ONT/USDT:USDT", "ZEC/USDT:USDT", "AUCTION/USDT:USDT", "GAS/USDT:USDT", "MEME/USDT:USDT", "BTC/USDT:USDT", "SUI/USDT:USDT", "CYBER/USDT:USDT", "LEVER/USDT:USDT", "MINA/USDT:USDT", "SUPER/USDT:USDT", "MANA/USDT:USDT", "FLM/USDT:USDT", "1000PEPE/USDT:USDT", "WAXP/USDT:USDT", "TOKEN/USDT:USDT", "SXP/USDT:USDT", "1000BONK/USDT:USDT", "VET/USDT:USDT", "APE/USDT:USDT", "DOT/USDT:USDT", "ARB/USDT:USDT", "JTO/USDT:USDT", "LINA/USDT:USDT", "BAKE/USDT:USDT", "ARK/USDT:USDT", "XRP/USDT:USDT", "METIS/USDT:USDT", "OCEAN/USDT:USDT", "MOVR/USDT:USDT", "CKB/USDT:USDT", "BOND/USDT:USDT", "MOODENG/USDT:USDT", "ILV/USDT:USDT", "ALICE/USDT:USDT", "SHIB1000/USDT:USDT", "RAD/USDT:USDT", "DOGE/USDT:USDT", "PHB/USDT:USDT", "PERP/USDT:USDT", "ARKM/USDT:USDT", "ZIL/USDT:USDT", "BNT/USDT:USDT", "ONDO/USDT:USDT", "FTM/USDT:USDT", "ORDI/USDT:USDT", "SKL/USDT:USDT", "MAGIC/USDT:USDT", "AVAX/USDT:USDT", "FXS/USDT:USDT", "STX/USDT:USDT", "ENS/USDT:USDT", "BOME/USDT:USDT", "GAL/USDT:USDT"]
        self.pairlist = tickers#ray.get(self.remote.fetch_bybit_copytrading_tickers.remote())
        # I know this is kinda stupid but its ok for now