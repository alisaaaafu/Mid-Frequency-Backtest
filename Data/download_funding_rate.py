import requests
import pandas as pd
import time
import os
from datetime import datetime, timezone

def get_funding_history(symbol, start_time=None, end_time=None, limit=1000):
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    params = {
        "symbol": symbol,
        "limit": limit
    }
    if start_time is not None:
        params["startTime"] = start_time
    if end_time is not None:
        params["endTime"] = end_time

    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")
        return []


def download_all_funding(symbol: str, start: datetime, end: datetime):
    if end is None:
        end = datetime.now(timezone.utc).date()

    start_time = int(start.timestamp() * 1000)  # Binance futures start
    end_time = int(end.timestamp() * 1000)
    funding_path = "/srv/data/BinanceU/funding"
    os.makedirs(funding_path, exist_ok=True)
    out_path = f"{funding_path}/Funding_BinanceU_{symbol}_perp.parquet"
    all_data = []

    while start_time < end_time:
        data = get_funding_history(symbol, start_time=start_time, end_time=end_time)
        if not data:
            break

        all_data.extend(data)
        last_time = int(data[-1]['fundingTime'])

        # Avoid tight looping, and move to next batch
        start_time = last_time + 1
        time.sleep(0.2)

    if all_data:
        df = pd.DataFrame(all_data)
        df['timestamp'] = pd.to_datetime(df['fundingTime'], unit='ms').dt.strftime("%Y-%m-%d %H:%M:%S")
        df.to_parquet(out_path, index=False)
        print(f"Saved {len(df)} funding rate records to: {out_path}")
    else:
        print("No funding data retrieved.")
