import argparse
import datetime
import os
import shutil
import pandas as pd

from Data.DataDownloader import download_and_unzip
from Data.download_funding_rate import download_all_funding

"""
This script is supposed to be running manually when we want to add one type 
of one contract that does not already exist.

example command to use:
assume you are in MFB dir, and venv is also in current dir
sudo -u vpnjob env PYTHONPATH=. ./venv/bin/python3 ./Data/bulk_download_binance.py --start_date 2019-12-01 --symbols BTCUSDT BTCUSDC
"""

COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "count",
    "taker_buy_volume", "taker_buy_quote_volume", "ignore"
]

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + datetime.timedelta(n)

def load_csv_with_optional_header(path, expected_columns):
    with open(path, 'r') as f:
        first_line = f.readline()
        f.seek(0)

        if any(col in first_line for col in expected_columns):
            return pd.read_csv(f)
        else:
            return pd.read_csv(f, header=None, names=expected_columns)

def download_history_range(market, contract, data_type, interval, symbol, start, end, extract_to):
    temp_dir = "/tmp/binance_dl"
    os.makedirs(temp_dir, exist_ok=True)

    current = start

    all_files = []
    while current < end:
        next_month = (current.replace(day=1) + datetime.timedelta(days=32)).replace(day=1)

        if next_month < end:
            # Monthly download
            year_month_str = current.strftime("%Y-%m")
            try:
                print(f"Downloading monthly: {year_month_str}")
                download_and_unzip(
                    market=market,
                    contract=contract,
                    freq='monthly',
                    data_type=data_type,
                    interval=interval,
                    symbol=symbol,
                    date=year_month_str,
                    extract_to=temp_dir
                )
                for f in os.listdir(temp_dir):
                    if f.startswith(f"{symbol}-{interval}-{year_month_str}") and f.endswith(".csv"):
                        all_files.append(os.path.join(temp_dir, f))
            except Exception as e:
                print(f"Skipping {year_month_str} (monthly): {e}")
            current = next_month
        else:
            # Final partial month — fallback to daily
            date_str = current.strftime("%Y-%m-%d")
            try:
                print(f"Downloading daily: {date_str}")
                download_and_unzip(
                    market=market,
                    contract=contract,
                    freq='daily',
                    data_type=data_type,
                    interval=interval,
                    symbol=symbol,
                    date=date_str,
                    extract_to=temp_dir
                )
                for f in os.listdir(temp_dir):
                    if f.startswith(f"{symbol}-{interval}-{date_str}") and f.endswith(".csv"):
                        all_files.append(os.path.join(temp_dir, f))
            except Exception as e:
                print(f"Skipping {date_str} (daily): {e}")
            current += datetime.timedelta(days=1)

    try:
        if all_files:
            all_files.sort()
            dfs = [load_csv_with_optional_header(f, COLUMNS) for f in all_files]
            combined = pd.concat(dfs)
            combined['timestamp'] = pd.to_datetime(combined['open_time'], unit='ms').astype(str)
            output_dir = os.path.join(extract_to, data_type, interval, symbol)
            os.makedirs(output_dir, exist_ok=True)

            out_path = os.path.join(output_dir, f"BinanceU_{symbol}_perp.parquet")
            combined.to_parquet(out_path, index=False)
            print(f"Combined data saved to: {out_path}")
        else:
            print("No files were successfully downloaded.")
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"Cleaned up temp directory: {temp_dir}")

def download_bvol_daily(market, data_type, symbol, start, end, extract_to):
    temp_dir = "/tmp/binance_dl"
    os.makedirs(temp_dir, exist_ok=True)

    current = start

    all_files = []
    while current < end:
        date_str = current.strftime("%Y-%m-%d")
        try:
            print(f"Downloading daily: {date_str}")
            download_and_unzip(
                market=market,
                freq='daily',
                data_type=data_type,
                symbol=symbol,
                date=date_str,
                extract_to=temp_dir
            )
            for f in os.listdir(temp_dir):
                if f.startswith(f"{symbol}-BVOLIndex-{date_str}") and f.endswith(".csv"):
                    all_files.append(os.path.join(temp_dir, f))
        except Exception as e:
            print(f"Skipping {date_str} (daily): {e}")
        current += datetime.timedelta(days=1)

    try:
        if all_files:
            all_files.sort()
            dfs = [pd.read_csv(f) for f in all_files]
            combined = pd.concat(dfs)
            combined['timestamp'] = pd.to_datetime(combined['calc_time'], unit='ms').astype(str)
            output_dir = os.path.join(extract_to, data_type, symbol)
            os.makedirs(output_dir, exist_ok=True)

            out_path = os.path.join(output_dir, f"BinanceU_{symbol}.parquet")
            combined.to_parquet(out_path, index=False)
            print(f"Combined data saved to: {out_path}")
        else:
            print("No files were successfully downloaded.")
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"Cleaned up temp directory: {temp_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and combine Binance historical data.")
    parser.add_argument("--market", default="futures", choices=["spot", "futures", "option"])
    parser.add_argument("--contract", default="um", choices=["um", "cm"])
    parser.add_argument("--data_type", default="klines")
    parser.add_argument("--interval", default="1m")
    parser.add_argument(
        "--symbols", nargs="+", default=["BTCUSDT"], 
        help="List of symbols to process (e.g. --symbols BTCUSDT ETHUSDT BNBUSDT)"
    )
    parser.add_argument("--start_date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--extract_to", default="/srv/data/BinanceU/")
    parser.add_argument("--mode", default="all", choices=['klines', "bvol", 'funding', 'all'], help="Data type to download")

    args = parser.parse_args()

    start = datetime.datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
    if args.end_date is None:
        end = datetime.datetime.now(datetime.timezone.utc)
    else:
        end = datetime.datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)

    for symbol in args.symbols:
        if args.mode in ["klines", "all"]:
            download_history_range(
                market=args.market,
                contract=args.contract,
                data_type=args.data_type,
                interval=args.interval,
                symbol=symbol,
                start=start,
                end=end,
                extract_to=args.extract_to
            )

        if args.mode in ["funding", "all"]:
            download_all_funding(
                symbol=symbol,
                start=start,
                end=end
            )
        
        if args.mode == "bvol":
            download_bvol_daily(
                market=args.market,
                data_type=args.data_type,
                symbol=symbol,
                start=start,
                end=end,
                extract_to=args.extract_to
            )
