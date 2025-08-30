# from asyncio import timeout

import aiohttp
import asyncio
import pandas as pd
import os
from collections import defaultdict
from time import time as titi

async def get_hyperliquid_trades(wallet_address, session = None):
    print(f"processing {wallet_address}")
    url = "https://api.hyperliquid.xyz/info"
    headers = {"Content-Type": "application/json"}
    body = {"type": "userFills", "user": wallet_address}
    if session is None:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=body, timeout=120) as response:
                response.raise_for_status()
                fills = await response.json()

    else:
        response = await session.post(url, headers=headers, json=body,timeout=120)
        response.raise_for_status()
        fills = response.json()

    time_start = titi()

    feather_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), f"my_data/data/caches/openbook/fills_{wallet_address}.feather")

    # feather_path =
    # loop = asyncio.get_event_loop()
    if os.path.exists(feather_path):
        historical_fills = pd.read_feather(feather_path)
    else:
        historical_fills = pd.DataFrame()

    new_fills_df = pd.DataFrame(fills)
    if not new_fills_df.empty:
        new_fills_df['time'] = pd.to_datetime(new_fills_df['time'], unit='ms', utc=True)
        new_fills_df = new_fills_df.set_index("tid")
        new_fills_df = new_fills_df[new_fills_df["dir"].isin(["Close Long", "Open Long", "Close Short", "Open Short", "Buy", "Sell"])]
        if not historical_fills.empty:
            for col in new_fills_df.columns:
                if col in historical_fills and new_fills_df[col].dtype != historical_fills[col].dtype:
                    new_fills_df[col] = new_fills_df[col].astype(historical_fills[col].dtype)
        all_fills = historical_fills.combine_first(new_fills_df)
    elif not historical_fills.empty:
        all_fills = historical_fills.copy()
    else:
        print(f"Code red - no fills detected for {wallet_address}")
        return None, None

    if "tid" in all_fills.columns:
        all_fills.index = all_fills['tid'].where(all_fills['tid'].notna(), all_fills.index)



    # cols_to_check = [col for col in all_fills.columns if col != 'tid']
    # print(cols_to_check)
    # print(all_fills[all_fills.index.duplicated(keep=False)])
    duplicate_counts = all_fills.index.value_counts()
    print(duplicate_counts[duplicate_counts > 1], "Are the duplicates ig")
    all_fills = all_fills[~all_fills.duplicated(subset=["sz", "time", "px", "dir", "coin", "side", "startPosition", "fee", "crossed", "hash"], keep='first')]


    all_fills.to_feather(feather_path)
    print(f"Saved all_fills to {feather_path}")
    print(all_fills)
    # print(all_fills[all_fills["hash"].isna()])

    all_fills = all_fills.sort_values(by="time").reset_index(drop=True)

    positions = defaultdict(lambda: {
        "long": {"size": 0.0, "average_entry_price": None, "open_time": None, "tradeItemId": None},
        "short": {"size": 0.0, "average_entry_price": None, "open_time": None, "tradeItemId": None}
    })
    closed_trades = []

    for _, fill in all_fills.iterrows():
        coin = fill["coin"]
        direction = fill["dir"]
        size = float(fill["sz"])
        price = float(fill["px"])
        time = fill["time"]
        txhash = str(fill['hash'])
        # print(time)

        if direction == "Open Long" or direction == "Buy":
            current = positions[coin]["long"]
            if current["size"] == 0:
                current["open_time"] = time
                current['tradeItemId'] = txhash
                # print(txhash)
            new_size = current["size"] + size
            current["average_entry_price"] = (
                price if current["average_entry_price"] is None
                else (current["size"] * current["average_entry_price"] + size * price) / new_size
            )
            current["size"] = new_size

        elif direction == "Close Long"  or direction == "Sell":
            current = positions[coin]["long"]
            if current["size"] > 0:  # Only reduce if long position exists
                size_to_close = min(size, current["size"])
                closed_trades.append({
                    "ticker": f"{coin}/USDT:USDT",
                    "open_time": current["open_time"],
                    "close_time": time,
                    "margin": size_to_close * current["average_entry_price"],
                    'open_price': current["average_entry_price"],
                    'close_price': price,
                    "leverage": 1,
                    "is_short": False,
                    "tradeItemId": current['tradeItemId'] + str(size_to_close * current["average_entry_price"])
                })
                current["size"] -= size_to_close
                if current["size"] == 0:
                    current["average_entry_price"] = None
                    current["open_time"] = None
                    current['tradeItemId'] = None

        elif direction == "Open Short":
            current = positions[coin]["short"]
            if current["size"] == 0:
                current["open_time"] = time
                current['tradeItemId'] = txhash
                # print(txhash)
            new_size = current["size"] + size
            current["average_entry_price"] = (
                price if current["average_entry_price"] is None
                else (current["size"] * current["average_entry_price"] + size * price) / new_size
            )
            current["size"] = new_size

        elif direction == "Close Short":
            current = positions[coin]["short"]
            if current["size"] > 0:  # Only reduce if short position exists
                size_to_close = min(size, current["size"])
                closed_trades.append({
                    "ticker": f"{coin}/USDT:USDT",
                    "open_time": current["open_time"],
                    "close_time": time,
                    "margin": size_to_close * current["average_entry_price"],
                    'open_price': current["average_entry_price"],
                    'close_price': price,
                    "leverage": 1,
                    "is_short": True,
                    "tradeItemId": current['tradeItemId'] + str(size_to_close * current["average_entry_price"])
                })
                # print(current['tradeItemId'])
                current["size"] -= size_to_close
                if current["size"] == 0:
                    current["average_entry_price"] = None
                    current["open_time"] = None
                    current['tradeItemId'] = None

    open_trades = []
    for coin in positions:
        if positions[coin]["long"]["size"] > 0:
            open_trades.append({
                "ticker": f"{coin}/USDT:USDT",
                "open_time": positions[coin]["long"]["open_time"],
                "close_time": None,
                "margin": positions[coin]["long"]["size"] * positions[coin]["long"]["average_entry_price"],
                'open_price': positions[coin]["long"]["average_entry_price"],
                'close_price': None,
                "leverage": 1,
                "is_short": False,
                "tradeItemId": (positions[coin]["long"]["tradeItemId"]) + str(positions[coin]["long"]["size"] * positions[coin]["long"]["average_entry_price"])
            })
        if positions[coin]["short"]["size"] > 0:
            open_trades.append({
                "ticker": f"{coin}/USDT:USDT",
                "open_time": positions[coin]["short"]["open_time"],
                "close_time": None,
                "margin": positions[coin]["short"]["size"] * positions[coin]["short"]["average_entry_price"],
                'open_price': positions[coin]["short"]["average_entry_price"],
                'close_price': None,
                "leverage": 1,
                "is_short": True,
                "tradeItemId": (positions[coin]["short"]["tradeItemId"]) + str(positions[coin]["short"]["size"] * positions[coin]["short"]["average_entry_price"])
            })

    columns = ["ticker", "open_time", "close_time", "margin", "open_price", "close_price", "leverage", "is_short", "tradeItemId"]
    closed_df = pd.DataFrame(closed_trades, columns=columns)
    open_df = pd.DataFrame(open_trades, columns=columns)
    
    open_df.set_index("tradeItemId",inplace=True)
    closed_df.set_index("tradeItemId",inplace=True)
    time_end = titi()

    print(f"Hyperliquid trades processed for {wallet_address} in {time_end - time_start}")
    return closed_df, open_df


if __name__ == "__main__":
    async def main():
        print(await get_hyperliquid_trades("0x83d038f06f65a2011e31434d15c656dace128153"))
    asyncio.run(main())