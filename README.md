# Mid-Frequency Backtest Engine

An event-driven backtesting framework for mid-frequency cryptocurrency trading strategies.

**Author:** Wanzhen Fu  
**Version:** 2.1.0

## Overview

This backtesting engine is designed for mid-frequency trading strategies in cryptocurrency markets. It provides realistic market simulation with support for multiple exchanges, order matching algorithms, and comprehensive data handling.

## Features

- Event-driven architecture for realistic market simulation
- Multiple strategy types: CTA, funding arbitrage, overnight, and portfolio strategies
- Advanced order matching and execution simulation
- Built-in technical indicators and time series operations
- Factor mining using genetic programming (DEAP)
- MongoDB integration for efficient data storage
- Support for Binance U-margined perpetual contracts

## Framework Components

- **Data Module**: Data downloading and preprocessing
- **Event Engine**: Central event processing system
- **Exchange Module**: Order matching and execution logic
- **Strategy Base**: Abstract strategy class and implementations
- **Trade Engine**: Position management and order routing
- **Time Series**: Technical indicators (MA, Pivot, etc.)
- **Research Tools**: Factor mining and analysis

## Quick Start

### Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Install MongoDB:
```
https://www.mongodb.com/docs/manual/administration/install-community/
```

### Running a Backtest

1. Create your strategy file in `Strategy/YOUR_STRATEGY.py`

2. Add configuration in `config.json`:
```json
{
  "YOUR_STRATEGY": {
    "strategy": "YOUR_STRATEGY",
    "symbol": "BTCUSDT",
    "start_date": "2024-01-01",
    "end_date": "2024-12-31"
  }
}
```

3. Run backtest:
```bash
python run_strategy.py -c YOUR_CONFIG
```

Results will be saved in `./bt_result/user/`

## Data

### Server Data Path

- Klines: `/srv/data/BinanceU/klines/1m/{symbol}/BinanceU_{symbol}_perp.parquet`
- Funding: `/srv/data/BinanceU/funding/Funding_BinanceU_{symbol}_perp.parquet`

### Supported Cryptocurrencies

BTCUSDT, BTCUSDC, ETHUSDT, ETHUSDC, BNBUSDT, BNBUSDC, SOLUSDT, SOLUSDC

## Research & Factor Mining

The framework includes tools for automated factor discovery:

```bash
python Research/signal_miner.py
python Research/single_factor_analysis.py
```

Features:
- Genetic programming for factor discovery
- Pre-built factor library
- Correlation analysis and intraday pattern analysis

## Testing

```bash
python -m pytest Data/test/
python -m pytest Strategy/test/
python -m pytest TSeries/test/
python -m pytest Trade/test/
```

## Configuration Files

- `config.json`: General strategy configurations
- `cta_config.json`: CTA-specific settings
- `funding_config.json`: Funding arbitrage settings
- `overnight_config.json`: Overnight strategy settings

---

**Note**: This framework is for research and backtesting purposes. Always validate strategies before live deployment.