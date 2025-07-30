import pytest
import os
import shutil
import hashlib
from pathlib import Path
from Data.DataDownloader import download_and_unzip  # Ensure your script is named `script.py`

# Test Parameters
TEST_SYMBOL = "BTCUSDT"
TEST_DATE = "2024-03-30"  # Choose a recent past date with available data
TEST_INTERVAL = "15m"  # Only used for klines
TEST_MARKET = "futures"
TEST_CONTRACT = "um"

# Supported Data Types
DATA_TYPES = [
    "aggTrades", "bookDepth", "bookTicker", "indexPriceKlines",
    "klines", "markPriceKlines", "metrics",
    "premiumIndexKlines", "trades"
]

@pytest.fixture(scope="function")
def temp_dir(tmp_path):
    """Setup: Create temporary directory. Teardown: Remove it after test."""
    yield tmp_path  # Provide the temp directory to the test
    shutil.rmtree(tmp_path)  # Cleanup after the test
    assert not tmp_path.exists(), "Teardown failed: Temporary directory still exists!"


def file_checksum(file_path):
    """Compute SHA-256 hash of a file (first 1MB for speed)."""
    with open(file_path, "rb") as f:
        return hashlib.sha256(f.read(1024 * 1024)).hexdigest()


@pytest.mark.parametrize("data_type", DATA_TYPES)
def test_download_and_unzip(temp_dir, data_type):
    """
    Test downloading and extracting each type of Binance data.
    The temp_dir fixture ensures cleanup after each test.
    """
    extract_to = temp_dir / data_type  # Unique directory per test
    extract_to.mkdir(parents=True, exist_ok=True)

    # Call the function to download and extract
    if data_type == "klines":
        download_and_unzip(
            market=TEST_MARKET,
            contract=TEST_CONTRACT,
            data_type=data_type,
            interval=TEST_INTERVAL,
            symbol=TEST_SYMBOL,
            date=TEST_DATE,
            extract_to=str(extract_to)
        )
    else:
        download_and_unzip(
            market=TEST_MARKET,
            contract=TEST_CONTRACT,
            data_type=data_type,
            symbol=TEST_SYMBOL,
            date=TEST_DATE,
            extract_to=str(extract_to)
        )

    # Ensure extracted directory is not empty
    extracted_files = list(Path(extract_to).glob("*.csv"))
    assert len(extracted_files) > 0, f"Extraction failed for {data_type}"

    print(f"Passed: {data_type} data downloaded and extracted successfully.")

def test_download_bvol_index(temp_dir):
    extract_to = temp_dir / "BVOLIndex"  # Unique directory per test
    extract_to.mkdir(parents=True, exist_ok=True)
    download_and_unzip(
            market="option",
            data_type="BVOLIndex",
            symbol="BTCBVOLUSDT",
            date=TEST_DATE,
            extract_to=str(extract_to)
        )
    extracted_files = list(Path(extract_to).glob("*.csv"))
    assert len(extracted_files) > 0, "Extraction failed for BVOLIndex"
    print("Passed: BVOLIndex data downloaded and extracted successfully.")
    