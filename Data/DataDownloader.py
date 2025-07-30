import argparse
import requests
import zipfile
import os

def download_and_unzip(market="futures", contract="um", freq="daily", data_type="klines", 
                       interval="15m", symbol="BTCUSDT", date="2024-03-30", 
                       extract_to="."):
    """
    Downloads and extracts Binance historical data.

    market - Choose "futures" or "spot".  
    contract - For futures only, choose "um" (USD-M Futures) or "cm" (COIN-M Futures). Ignored for spot.  
    data_type - Type of data to download: "klines", "trades", "aggTrades", "liquidation", etc.  
    interval - Timeframe of data (used only for "klines", ignored for others).  
    symbol - Trading pair (e.g., "BTCUSDT", "ETHUSDT").  
    date - Date in YYYY-MM-DD format.  
    extract_to - Directory where the extracted data will be saved.  
    """

    # Construct the URL based on data type
    base_url = "https://data.binance.vision/data"
    interval_list = ["klines", "markPriceKlines", "indexPriceKlines", "premiumIndexKlines"]
    no_interval_list = ["trades", "aggTrades", "bookDepth", "bookTicker", "metrics"]

    if market == "spot":
        if data_type in interval_list:
            url = f"{base_url}/spot/{freq}/{data_type}/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"
        elif data_type in no_interval_list:
            url = f"{base_url}/spot/{freq}/{data_type}/{symbol}/{symbol}-{data_type}-{date}.zip"
        else:
            raise ValueError(f"Data type '{data_type}' is not available for spot market.")
    elif market == "futures":
        if data_type in interval_list:
            url = f"{base_url}/futures/{contract}/{freq}/{data_type}/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"
        elif data_type in no_interval_list:
            url = f"{base_url}/futures/{contract}/{freq}/{data_type}/{symbol}/{symbol}-{data_type}-{date}.zip"
        else:
            raise ValueError(f"Data type '{data_type}' is not supported.")
    elif market == "option":
        if data_type == "BVOLIndex":
            url = f"{base_url}/option/{freq}/BVOLIndex/{symbol}/{symbol}-BVOLIndex-{date}.zip"
        else:
            raise ValueError("Invalid data type. Only support BVOLIndex.")
    else:
        raise ValueError("Invalid market type. Choose 'spot', 'futures'.")

    os.makedirs(extract_to, exist_ok=True)
    file_name = os.path.join(extract_to, url.split("/")[-1]) 
    print(f"Downloading from: {url}")

    # Download the file
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(file_name, "wb") as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)

        # Unzip the file
        with zipfile.ZipFile(file_name, "r") as zip_ref:
            zip_ref.extractall(extract_to)

        # Optionally, delete the zip file after extraction
        os.remove(file_name)
    else:
        print(f"Failed to download {file_name}. Check if the date and parameters are correct.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and extract Binance historical data.")
    
    parser.add_argument("--market", type=str, default="futures", choices=["spot", "futures", "option"], help="Market type: 'spot', 'futures' or 'option")
    parser.add_argument("--contract", type=str, default="um", choices=["um", "cm"], help="Futures contract type: 'um' (USD-M) or 'cm' (COIN-M)")
    parser.add_argument("--data_type", type=str, default="klines", choices=["aggTrades", "bookDepth", "bookTicker", "indexPriceKlines",
                                                                            "klines", "liquidationSnapshot", "markPriceKlines", "metrics",
                                                                            "premiumIndexKlines", "trades", "BVOLIndex"], help="Type of data to download")
    parser.add_argument("--freq", type=str, default="daily", choices=["daily", "monthly"], 
                        help="Aggregation frequency of data (e.g. 'daily', 'monthly')")
    parser.add_argument("--interval", type=str, default="15m", help="Timeframe (used for 'klines', ignored for others)")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Trading pair (e.g., 'BTCUSDT', 'ETHUSDT')")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--extract_to", type=str, default=".", help="Directory where extracted data will be saved")

    args = parser.parse_args()

    # Call function with parsed arguments
    download_and_unzip(
        market=args.market,
        contract=args.contract,
        freq=args.freq,
        data_type=args.data_type,
        interval=args.interval,
        symbol=args.symbol,
        date=args.date,
        extract_to=args.extract_to
    )
