import os
import ccxt
import pandas as pd
from datetime import datetime, timedelta, timezone
import pyarrow.feather as feather
import time


def _ts_to_str(timestamp_ms, ex=None):  
    """Converts a millisecond timestamp to a human-readable UTC string."""
    if timestamp_ms is None: return "None"
    if ex and hasattr(ex, 'iso8601'):
        try:
            return ex.iso8601(int(timestamp_ms))
        except Exception:
            pass
    return pd.to_datetime(timestamp_ms, unit='ms', utc=True).strftime('%Y-%m-%d %H:%M:%S %Z')


def _fetch_ohlcv_with_retry(exchange, pair, timeframe_str, since_ts, limit_val, context_str="", params=None):
    if params is None:
        params = {}

    ohlcv_data = None
    fetch_success = False
    max_retries = 5
    fetch_since_ts = int(since_ts) if since_ts is not None else None

    for attempt in range(max_retries):
        try:
            since_log = _ts_to_str(fetch_since_ts,
                                   exchange) if fetch_since_ts is not None else "None (fetch most recent)"
            endTime_log = _ts_to_str(params.get('endTime'), exchange) if params.get('endTime') else "Not Used"
            print(
                f"    {context_str} Attempt {attempt + 1}/{max_retries}: Fetching for {pair} (tf: {timeframe_str}) from {since_log} with limit {limit_val}, endTime: {endTime_log}...")

            ohlcv_data = exchange.fetch_ohlcv(pair, timeframe_str, since=fetch_since_ts, limit=limit_val, params=params)
            fetch_success = True
            break
        except ccxt.RateLimitExceeded as e:
            print(f"    {context_str} Rate limit exceeded: {e}. Sleeping 60s...")
            time.sleep(60)
        except ccxt.NetworkError as e:
            wait_time = 2 ** (attempt)
            print(f"    {context_str} Network error: {e}. Sleeping {wait_time}s...")
            time.sleep(wait_time)
        except ccxt.ExchangeError as e:
            print(f"    {context_str} Exchange error: {e}.")
            err_lower = str(e).lower()
            if "unknown contract" in err_lower or \
                    "symbol invalid" in err_lower or \
                    "parameter" in err_lower and "err" in err_lower or \
                    "not supported" in err_lower or \
                    "the start time cannot be earlier than" in err_lower or \
                    "timestamp is out of range" in err_lower:
                print(
                    f"    {context_str} Error suggests non-retryable issue for {pair} with current parameters or data range. Stopping fetch for this segment.")
                fetch_success = False
                if "the start time cannot be earlier than" in err_lower or "timestamp is out of range" in err_lower:
                    ohlcv_data = []
                    fetch_success = True
                break
            wait_time = 2 ** (attempt)
            print(f"    {context_str} Retrying exchange error. Sleeping {wait_time}s...")
            time.sleep(wait_time)
        except Exception as e:
            print(f"    {context_str} Unexpected error during fetch: {e}.")
            fetch_success = False
            break
    return ohlcv_data, fetch_success


def download_past_data(pairs, exchange_name, timeframe_minutes, day_count=400):
    if not pairs:
        print("No pairs provided. Exiting.")
        return

    userdir = './my_data/data/bybit/futures'

    try:
        exchange_class = getattr(ccxt, exchange_name)
        exchange = exchange_class({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })
        print(f"Initialized CCXT exchange: {exchange_name}")
    except AttributeError:
        print(f"Error: Exchange '{exchange_name}' not found in ccxt.")
        return
    except Exception as e:
        print(f"Error initializing exchange {exchange_name}: {e}")
        return

    try:
        exchange.load_markets()
    except (ccxt.NetworkError, ccxt.ExchangeError, Exception) as e:
        print(f"Error loading markets for {exchange_name}: {e}")
        return

    datetime_now = datetime.now(timezone.utc)
    end_ts = exchange.parse8601(datetime_now.isoformat())
    datetime_prev = datetime_now - timedelta(days=day_count)
    overall_start_ts = exchange.parse8601(datetime_prev.isoformat())

    print(f"Overall target time range: [{_ts_to_str(overall_start_ts, exchange)}, {_ts_to_str(end_ts, exchange)})")

    if timeframe_minutes >= 1440 and timeframe_minutes % 1440 == 0:
        timeframe_str = f'{timeframe_minutes // 1440}d'
    elif timeframe_minutes >= 60 and timeframe_minutes % 60 == 0:
        timeframe_str = f'{timeframe_minutes // 60}h'
    else:
        timeframe_str = f'{timeframe_minutes}m'

    if timeframe_str not in exchange.timeframes:
        print(
            f"Error: Timeframe '{timeframe_str}' not supported by {exchange_name}. Supported: {list(exchange.timeframes.keys())}")
        return

    limit = exchange.safe_integer(exchange.limits.get('fetchOHLCV', {}), 'max', 1000)
    timeframe_ms = exchange.parse_timeframe(timeframe_str) * 1000

    for pair in pairs:
        print(f"\nProcessing pair: {pair}")

        if pair not in exchange.markets:
            print(f"Warning: Pair '{pair}' not found in {exchange_name} markets. Skipping.")
            continue

        safe_pair = pair.replace('/', '_').replace(':', '_')
        filename = os.path.join(userdir, f'{safe_pair}-{timeframe_str}-futures.feather')

        existing_df = None
        first_existing_ts_ms = None
        last_existing_ts_ms = None

        if os.path.exists(filename):
            try:
                existing_df = feather.read_feather(filename)
                if not existing_df.empty and isinstance(existing_df.index, pd.DatetimeIndex):
                    if existing_df.index.tz is None:
                        print("  Existing DataFrame index is timezone naive. Localizing to UTC.")
                        existing_df.index = existing_df.index.tz_localize(timezone.utc)
                    elif existing_df.index.tz != timezone.utc:
                        print(
                            f"  Existing DataFrame index has timezone {existing_df.index.tz}. Converting to standard UTC.")
                        existing_df.index = existing_df.index.tz_convert(timezone.utc)

                    existing_df = existing_df[~existing_df.index.duplicated(keep='first')].sort_index()

                    if not existing_df.empty:
                        first_existing_ts_ms = existing_df.index[0].value // 1_000_000
                        last_existing_ts_ms = existing_df.index[-1].value // 1_000_000
                        print(
                            f"  Existing data loaded: {_ts_to_str(first_existing_ts_ms, exchange)} to {_ts_to_str(last_existing_ts_ms, exchange)} ({len(existing_df)} records).")
                    else:
                        print(
                            f"  Existing file {filename} was empty or contained no valid data after initial load/sort/dedupe.")
                        existing_df = None
                elif existing_df.empty:
                    print(f"  Existing file {filename} is empty. Will fetch fresh.")
                    existing_df = None
                else:
                    print(f"  Existing file {filename} has invalid index or format. Will overwrite.")
                    existing_df = None
            except Exception as e:
                print(f"  Error reading or processing existing file {filename}: {e}. Will overwrite.")
                existing_df = None
        else:
            print(
                f"  No existing data file found at {filename}. Will fetch from {_ts_to_str(overall_start_ts, exchange)}.")

        fetch_tasks = []

        
        if existing_df is not None and not existing_df.empty:
            if first_existing_ts_ms <= overall_start_ts and last_existing_ts_ms >= end_ts:
                print(
                    f"  Existing data fully covers target range [{_ts_to_str(overall_start_ts, exchange)}, {_ts_to_str(end_ts, exchange)}). No fetch operations needed.")
                continue  

        if existing_df is None or existing_df.empty:
            if overall_start_ts < end_ts:
                print(
                    f"  Task: Initial Full Fetch for range [{_ts_to_str(overall_start_ts, exchange)}, {_ts_to_str(end_ts, exchange)})")
                fetch_tasks.append((overall_start_ts, end_ts, "Initial Full Fetch"))
        else:
            existing_df_index_sorted = existing_df.index

            if overall_start_ts < first_existing_ts_ms:
                prefetch_until_ts = min(first_existing_ts_ms, end_ts)
                if overall_start_ts < prefetch_until_ts:
                    print(
                        f"  Task: Prefetch for range [{_ts_to_str(overall_start_ts, exchange)}, {_ts_to_str(prefetch_until_ts, exchange)})")
                    fetch_tasks.append((overall_start_ts, prefetch_until_ts, "Prefetch"))

            for i in range(len(existing_df_index_sorted) - 1):
                current_block_end_dt = existing_df_index_sorted[i]
                next_block_start_dt = existing_df_index_sorted[i + 1]
                current_block_end_ts = current_block_end_dt.value // 1_000_000
                next_block_start_ts = next_block_start_dt.value // 1_000_000
                gap_fill_start_candidate_ts = current_block_end_ts + timeframe_ms

                if gap_fill_start_candidate_ts < next_block_start_ts:
                    actual_gap_fetch_start_ts = max(overall_start_ts, gap_fill_start_candidate_ts)
                    actual_gap_fetch_end_ts = min(end_ts, next_block_start_ts)
                    if actual_gap_fetch_start_ts < actual_gap_fetch_end_ts:
                        print(
                            f"  Task: Internal Gap Fill for range [{_ts_to_str(actual_gap_fetch_start_ts, exchange)}, {_ts_to_str(actual_gap_fetch_end_ts, exchange)})")
                        fetch_tasks.append((actual_gap_fetch_start_ts, actual_gap_fetch_end_ts, "Internal Gap Fill"))

            postfetch_start_candidate_ts = last_existing_ts_ms + timeframe_ms
            actual_postfetch_start_ts = max(overall_start_ts, postfetch_start_candidate_ts)
            if actual_postfetch_start_ts < end_ts:
                print(
                    f"  Task: Postfetch for range [{_ts_to_str(actual_postfetch_start_ts, exchange)}, {_ts_to_str(end_ts, exchange)})")
                fetch_tasks.append((actual_postfetch_start_ts, end_ts, "Postfetch"))

        fetch_tasks.sort(key=lambda x: x[0])

        all_newly_fetched_ohlcv_raw = []

        if not fetch_tasks:
            print(
                "  No fetch operations planned. Data is likely up-to-date for the requested range or no new data segments identified.")

        for since_request_ts, until_request_exclusive_ts, context in fetch_tasks:
            print(
                f"  Executing fetch task: {context} from {_ts_to_str(since_request_ts, exchange)} up to (but not including) {_ts_to_str(until_request_exclusive_ts, exchange)}")

            current_since_fetch = since_request_ts
            while current_since_fetch < until_request_exclusive_ts:
                previous_fetch_start_ts_for_loop_check = current_since_fetch

                fetch_params = {}  
                
                if context not in ["Prefetch", "Initial Full Fetch"]:
                    fetch_params['endTime'] = int(until_request_exclusive_ts)

                ohlcv_segment, fetch_success = _fetch_ohlcv_with_retry(
                    exchange, pair, timeframe_str, current_since_fetch, limit, context, params=fetch_params
                )

                if not fetch_success:
                    print(f"    {context}: Failed to fetch segment for {pair} after retries. Skipping this fetch task.")
                    break

                if not ohlcv_segment:
                    print(
                        f"    {context}: No more data returned by exchange for period starting {_ts_to_str(current_since_fetch, exchange)} within task range.")
                    
                    if current_since_fetch < (exchange.milliseconds() - timedelta(
                            days=max(1, int(0.1 * day_count))).total_seconds() * 1000) and \
                            len(ohlcv_segment) < limit:
                        
                        jump_factor = 100 if (day_count > 300 and timeframe_minutes == 1) else 10
                        jump_amount_ms = limit * timeframe_ms * jump_factor
                        print(
                            f"    {context}: Current 'since' ({_ts_to_str(current_since_fetch, exchange)}) is old & no results. Advancing 'since' by ~{jump_amount_ms // 1000 // 3600} hours to probe further.")
                        current_since_fetch += jump_amount_ms
                        if current_since_fetch >= until_request_exclusive_ts: break
                        continue
                    break

                segment_to_add = [
                    c for c in ohlcv_segment
                    if since_request_ts <= c[0] < until_request_exclusive_ts and \
                       overall_start_ts <= c[0] < end_ts
                ]

                if segment_to_add:
                    all_newly_fetched_ohlcv_raw.extend(segment_to_add)
                    print(
                        f"    {context}: Fetched {len(ohlcv_segment)}, kept {len(segment_to_add)} candles. Kept TS from {_ts_to_str(segment_to_add[0][0], exchange)} to {_ts_to_str(segment_to_add[-1][0], exchange)}")

                last_fetched_ts_in_raw_segment = ohlcv_segment[-1][0]
                current_since_fetch = last_fetched_ts_in_raw_segment + 1

                if current_since_fetch <= previous_fetch_start_ts_for_loop_check:
                    print(
                        f"    {context} Warning: Fetch start timestamp did not advance significantly (is {_ts_to_str(current_since_fetch, exchange)}, was {_ts_to_str(previous_fetch_start_ts_for_loop_check, exchange)}). Last fetched raw TS: {_ts_to_str(last_fetched_ts_in_raw_segment, exchange)}. Stopping fetch for this task to prevent loop.")
                    break

                if current_since_fetch >= until_request_exclusive_ts:
                    print(
                        f"    {context}: Next fetch start point {_ts_to_str(current_since_fetch, exchange)} meets or exceeds task's end time {_ts_to_str(until_request_exclusive_ts, exchange)}. Task complete.")
                    break

        newly_fetched_df = None
        if all_newly_fetched_ohlcv_raw:
            try:
                newly_fetched_df = pd.DataFrame(all_newly_fetched_ohlcv_raw,
                                                columns=['date', 'open', 'high', 'low', 'close', 'volume'])
                newly_fetched_df['date'] = pd.to_datetime(newly_fetched_df['date'], unit='ms', utc=True)
                newly_fetched_df.set_index('date', inplace=True)
                newly_fetched_df = newly_fetched_df[~newly_fetched_df.index.duplicated(keep='first')]
                if not newly_fetched_df.empty:
                    print(f"  Converted {len(newly_fetched_df)} newly fetched unique candles to DataFrame.")
                else:
                    newly_fetched_df = None
            except Exception as e:
                print(f"  Error converting newly fetched data to DataFrame for {pair}: {e}.")
                newly_fetched_df = None

        dfs_to_combine = []
        if existing_df is not None and not existing_df.empty:
            dfs_to_combine.append(existing_df)
        if newly_fetched_df is not None and not newly_fetched_df.empty:
            dfs_to_combine.append(newly_fetched_df)

        if not dfs_to_combine:
            print(f"  No data (new or existing) to process for {pair}. Skipping save.")
            continue

        final_df = pd.concat(dfs_to_combine)
        final_df = final_df[~final_df.index.duplicated(keep='first')]
        final_df = final_df.sort_index()
        print(f"  Combined DataFrame now has {len(final_df)} unique candles after deduplication and sort.")

        # min_target_dt = pd.to_datetime(overall_start_ts, unit='ms', utc=True)
        # max_target_dt_exclusive = pd.to_datetime(end_ts, unit='ms', utc=True)

        # original_len_before_trim = len(final_df)
        # final_df = final_df[(final_df.index >= min_target_dt) & (final_df.index < max_target_dt_exclusive)]

        # if len(final_df) < original_len_before_trim:
        #     print(
        #         f"  Trimmed final DataFrame to the range [{_ts_to_str(overall_start_ts, exchange)}, {_ts_to_str(end_ts, exchange)}). Removed {original_len_before_trim - len(final_df)} out-of-bounds candles.")

        if final_df.empty:
            print(f"  Final DataFrame is empty after all processing for {pair}. No data to save.")
            continue

        try:
            os.makedirs(userdir, exist_ok=True)
            feather.write_feather(final_df, filename)
            first_saved_ts = final_df.index[0].value // 1_000_000
            last_saved_ts = final_df.index[-1].value // 1_000_000
            print(
                f"  Successfully saved data ({len(final_df)} candles) from {_ts_to_str(first_saved_ts, exchange)} to {_ts_to_str(last_saved_ts, exchange)} to: {filename}")
        except Exception as e:
            print(f"  Error saving final data to Feather file {filename}: {e}")

    print('\nDownload process complete.')







download_past_data(pairs=['BTC/USDT:USDT'],
                   exchange_name='bybit',
                   timeframe_minutes=1440, 
                   day_count=5000)