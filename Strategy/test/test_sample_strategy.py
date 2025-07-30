import pytest
import os
import shutil
import filecmp
import glob

from run_strategy import run_strategy

@pytest.fixture
def clean_bt_result():
    result_base = "./bt_result/pytest"
    if os.path.exists(result_base):
        shutil.rmtree(result_base)
    os.makedirs(result_base, exist_ok=True)
    yield result_base
    shutil.rmtree(result_base)

def collect_csv_files(base_dir):
    """
    Return a dictionary of relative path => full path for all CSV files.
    """
    files = glob.glob(os.path.join(base_dir, "**", "*.csv"), recursive=True)
    return {
        os.path.relpath(f, base_dir): f
        for f in files
    }

def test_run_sample_strategy(clean_bt_result):
    # Run strategy with sample config
    test_config = "Strategy/test/sample_strategy_config.json"
    try:
        run_strategy(config_path=test_config)
    except SystemExit as e:
        assert e.code == 0, f"Program exited with error code: {e.code}"

    # Find the latest timestamped output directory under ./bt_result/pytest/
    subdirs = [
        os.path.join(clean_bt_result, d)
        for d in os.listdir(clean_bt_result)
        if os.path.isdir(os.path.join(clean_bt_result, d))
    ]
    assert subdirs, "No output directory found under ./bt_result/pytest/"
    output_dir = max(subdirs, key=os.path.getmtime)

    # Load all .csv files from output and expected dirs
    actual_files = collect_csv_files(output_dir)
    expected_dir = "Strategy/test/expected_output"
    expected_files = collect_csv_files(expected_dir)

    # Ensure all expected files exist in actual output
    assert actual_files.keys() == expected_files.keys(), \
        f"Output files mismatch.\nExpected: {sorted(expected_files.keys())}\nActual: {sorted(actual_files.keys())}"

    # Compare file content
    for rel_path in expected_files:
        actual = actual_files[rel_path]
        expected = expected_files[rel_path]
        assert filecmp.cmp(actual, expected, shallow=False), f"Mismatch in file: {rel_path}"