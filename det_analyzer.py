import logging

# from NostalgiaForInfinityImport.tests.backtests.test_winrate_and_drawdown import backtest

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

from backtester import Backtester
# import ray
import os
# from dotenv import load_dotenv
import asyncio
import polars as pl
import time
import pandas as pd

# load_dotenv()

is_cloud = not os.environ.get("ISCLOUD", "false").lower() == "false"
folder = os.path.realpath("/mnt/gc") if is_cloud else os.path.realpath(
    os.path.join(os.path.dirname(__file__), "my_data/data/"))


def list_files_in_bucket(bucket_path):
    try:
        files = []
        for root, dirs, file_names in os.walk(bucket_path):
            for file_name in file_names:
                file_path = os.path.join(root, file_name)
                files.append(file_path)
        return files
    except Exception as e:
        return str(e)


async def process_dataframe(df):
    blacklist = backtest_engine.backtest_cacher.fetch_blacklist_extended()
    logger.info(f"Current blacklist: {blacklist}, current df height: {len(df)}")
    df = df.filter(~pl.col("Trader Address").is_in(blacklist))
    logger.info(f"Filtered df height: {len(df)}")

    
    tasks = [backtest_engine.open_book.bybit_check_availability(row["Trader Address"], row["tagname"], asyncio.Semaphore(10)) for
             row in df.iter_rows(named=True) if row["type"] == "bybit"]

    
    for task in asyncio.as_completed(tasks):
        trader_address, tagname, can_view = await task
        if not can_view:
            backtest_engine.backtest_cacher.add_to_blacklist({"type": "bybit", "Trader Address": trader_address, "tagname": tagname})
            df = df.filter(~pl.col("Trader Address").is_in([trader_address]))
            logger.info(f"Removing {trader_address} from df and adding to blacklist as is unviewable")

    return df


def get_new_hl_wallets():
    
    df = pd.read_csv('hl_trader_ratios.csv')
    

    
    rows = []
    batch_stats = []  
    for _, row in df.iterrows():
        addr = row['Trader Address']
        name = row.get('name', '') if 'name' in row else ''
        pnlat = float(row['allTime_PNL']) if not pd.isna(row['allTime_PNL']) else 0.0
        pnlmonth = float(row['month_PNL']) if 'month_PNL' in row and not pd.isna(row['month_PNL']) else 0.0
        maxaccval = float(row['MaxACCValue']) if not pd.isna(row['MaxACCValue']) else 0.0
        accval = float(row['ACCValue']) if not pd.isna(row['ACCValue']) else 0.0
        rating = float(row['rating'])
        tagname = (str(name) if not pd.isna(name) else '') + (addr if pd.isna(name) else '') + '; PNL:' + str(pnlat)

        
        batch_stats.append({'Trader_Address': addr, 'typer': 'hl', 'col': 'tagname', 'value': tagname})
        batch_stats.append({'Trader_Address': addr, 'typer': 'hl', 'col': 'PNL', 'value': pnlat})
        batch_stats.append({'Trader_Address': addr, 'typer': 'hl', 'col': 'officialR', 'value': rating})
        batch_stats.append({'Trader_Address': addr, 'typer': 'hl', 'col': 'PNLMonth', 'value': pnlmonth})
        batch_stats.append({'Trader_Address': addr, 'typer': 'hl', 'col': 'ACCValue', 'value': accval})
        batch_stats.append({'Trader_Address': addr, 'typer': 'hl', 'col': 'MaxACCValue', 'value': maxaccval})

        rows.append({
            'Trader Address': addr,
            'tagname': tagname,
            'type': 'hl',
            "rating": 100000.0
        })

    
    backtest_engine.backtest_cacher.batch_set_official_stats_vectorized(batch_stats)
    df = df[df['rating'] > 0]

    result = pl.from_pandas(pd.DataFrame(rows))
    return result



async def cron_job_entrypoint():
    while True:
        # logger.info(f"ISCLOUD: {is_cloud}")
        # files = list_files_in_bucket(folder)
        # if len(files) == 0:
        #     logger.error("No files found in supposed bucket folder")
        # bybit_traders =  ray.get(backtest_engine.open_book.remote.bybit_fetch_ranks.remote(50))#await backtest_engine.open_book.bybit_fetch_ranks(50)
        # okx_traders = ray.get(backtest_engine.open_book.remote.okx_fetch_ranks.remote(50))#await backtest_engine.open_book.okx_fetch_ranks(50)
        hype_traders = get_new_hl_wallets()
        print(hype_traders, "traders received from the csv files")
        print(hype_traders.filter((pl.col("Trader Address").eq("0x2381c59b0e7e83853ec472b4e45303e08fc006e2"))))
        # print(hype_traders[hype_traders['uid'] == "0x2381c59b0e7e83853ec472b4e45303e08fc006e2"])
        previous_traders = backtest_engine.backtest_cacher.retrieve_good_traders()
        traders = pl.concat([i for i in [previous_traders.select(["Trader Address", "tagname", "type", "rating"]), hype_traders] if i is not None])
        traders = traders.unique(subset=["type", "Trader Address"])
        print(traders)
        filtered_traders = await process_dataframe(traders)

        if 'rating' in filtered_traders.columns:
            logger.info("Sequencing backtests by rating (descending).")
            filtered_traders = filtered_traders.sort("rating", descending=True)
        else:
            logger.warning("Could not find 'rating' column. Backtesting in default order.")

        # exit(0)

        for trader in filtered_traders.iter_rows(named=True):
            backtest_engine.reset_downloaded_queue()
            logger.info(f"Starting backtest of trader on {trader['type']}: {trader['tagname']}({trader['Trader Address']})")
            time1 = time.time()
            try:
                response = await backtest_engine.backtest_traders(
                    [{k: v for k, v in trader.items() if k in ["type", "Trader Address", "tagname"]}])
                logger.info(
                    f"Finished backtest of trader on {trader['type']}: {trader['tagname']}({trader['Trader Address']}) in {(time.time() - time1):.2f}")
                # for mode in response["modes"]:
                print(response['ratios'])
                    # logger.info(
                    #     f"{mode}: Sharpe: {response['ratios'][f'sharpe_ratio_{mode}']}, Calmar: {response['ratios'][f'calmar_ratio_{mode}']}, Days traded: {response['ratios'][f'days_traded_{mode}']}")
            except Exception as e:
                logger.exception(
                    f"Exception has occurred while backtesting trader on {trader['type']}: {trader['tagname']}({trader['Trader Address']}), {e}")
        logger.info(f"Finished cron job. Done checking out {filtered_traders.height} traders. Waiting for 5 hours.")
        time.sleep(60*60*5)


if __name__ == "__main__":
    logger.info(f"Base folder is {folder}")

    backtest_engine = Backtester(main_path=folder, cache_file_name="caches.csv", namespace="apple", semaphore_count=1)

    asyncio.run(cron_job_entrypoint())