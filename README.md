# Mid-Frequency Backtest Engine

A comprehensive event-driven backtesting framework designed for mid-frequency cryptocurrency trading strategies.

**Author:** Wanzhen Fu  
**Version:** 2.1.0

## Overview

This is a production-ready backtesting engine built specifically for mid-frequency trading strategies in cryptocurrency markets. The framework provides a realistic simulation environment with support for multiple exchanges, order matching algorithms, and comprehensive data handling capabilities.

## Features

- **Event-Driven Architecture**: Flexible and scalable event-driven design for realistic market simulation
- **Multiple Strategy Types**: Support for CTA, funding arbitrage, overnight, and portfolio strategies
- **Advanced Order Matching**: Realistic order execution simulation with various matching algorithms
- **Time Series Analysis**: Built-in technical indicators and time series operations (MA, Pivot, etc.)
- **Factor Mining**: Automated factor discovery and analysis using genetic programming (DEAP)
- **MongoDB Integration**: Efficient data storage and retrieval for historical market data
- **Multi-Exchange Support**: Currently supports Binance U-margined perpetual contracts
- **Comprehensive Testing**: Unit tests for critical components

## Framework Architecture

### Core Components

- **Data Module** (`Data/`): Data downloading, preprocessing, and handling from multiple exchanges
- **Event Engine** (`Event_Engine.py`): Central event processing system for market data and orders
- **Exchange Module** (`Exchange/`): Exchange-specific implementations and order matching logic
- **Strategy Base** (`Strategy/`): Abstract strategy class and sample implementations
- **Trade Engine** (`Trade/`): Position management, order routing, and execution optimization
- **Time Series** (`TSeries/`): Technical indicators and time series operations
- **Research Tools** (`Research/`): Factor zoo, signal mining, and backtesting analysis

### Strategy Types

1. **CTA Strategy**: Trend-following and momentum-based strategies
2. **Funding Arbitrage**: Exploit funding rate differentials
3. **Overnight Strategy**: Capture overnight price movements
4. **Portfolio Strategy**: Multi-asset portfolio management

## Quick Start

### Prerequisites

1. **Install Python Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Install MongoDB**
   
   MongoDB is required for data storage. Follow the official installation guide:
   ```
   https://www.mongodb.com/docs/manual/administration/install-community/
   ```

3. **(Optional) Install DataGrip**
   
   A powerful cross-platform database IDE for managing MongoDB:
   ```
   https://www.jetbrains.com/datagrip/download/
   ```

### Running Your First Backtest

1. **Create Your Strategy**
   
   Create a new strategy file under the `Strategy` folder:
   ```
   ./Strategy/YOUR_STRATEGY.py
   ```
   
   You can use `sample_strategy.py` or `strategy_cta_sample.py` as templates.

2. **Configure Your Strategy**
   
   Add your strategy configuration in `config.json`:
   ```json
   {
     "YOUR_STRATEGY": {
       "strategy": "YOUR_STRATEGY",
       "symbol": "BTCUSDT",
       "start_date": "2024-01-01",
       "end_date": "2024-12-31",
       ...
     }
   }
   ```

3. **Run the Backtest**
   ```bash
   python run_strategy.py -c YOUR_CONFIG
   ```
   
   Results will be saved in `./bt_result/user/` with detailed trade logs and performance metrics.

## Data Management

### Supported Data Types

- **Klines Data**: OHLCV candlestick data at 1-minute resolution
- **Funding Rate Data**: Historical funding rates for perpetual contracts
- **Order Book Data**: (Coming soon)

### Data Path on Server

For users with access to the Cosmos server:

- Klines: `/srv/data/BinanceU/klines/1m/{symbol}/BinanceU_{symbol}_perp.parquet`
- Funding: `/srv/data/BinanceU/funding/Funding_BinanceU_{symbol}_perp.parquet`

### Currently Supported Cryptocurrencies

- BTC: BTCUSDT, BTCUSDC
- ETH: ETHUSDT, ETHUSDC
- BNB: BNBUSDT, BNBUSDC
- SOL: SOLUSDT, SOLUSDC

## Research & Factor Mining

The framework includes powerful research tools for factor discovery and analysis:

### Factor Mining

- **Genetic Programming**: Automatically discover profitable factors using DEAP library
- **Factor Zoo**: Pre-built library of common technical factors
- **Custom Operators**: Build complex factors from basic building blocks

### Single Factor Analysis

- Correlation analysis between factors and future returns
- Intraday pattern analysis
- Factor performance metrics and visualization

Usage example:
```bash
python Research/signal_miner.py
python Research/single_factor_analysis.py
```

## Project Structure

```
Mid-Frequency-backtest/
├── Data/                    # Data downloading and handling
├── Exchange/                # Exchange implementations
├── Strategy/                # Strategy implementations
├── Trade/                   # Order execution and position management
├── TSeries/                 # Time series indicators
├── Research/                # Factor mining and analysis
├── Utils/                   # Utility functions and constants
├── Preprocess/              # Data preprocessing scripts
├── run_strategy.py          # Main entry point for backtesting
└── config.json              # Strategy configurations
```

## Testing

Run unit tests to ensure everything is working correctly:

```bash
# Test data downloading
python -m pytest Data/test/

# Test strategy execution
python -m pytest Strategy/test/

# Test time series operations
python -m pytest TSeries/test/

# Test trade engine
python -m pytest Trade/test/
```

## Configuration Files

- `config.json`: General strategy configurations
- `cta_config.json`: CTA-specific settings
- `funding_config.json`: Funding arbitrage settings
- `overnight_config.json`: Overnight strategy settings

## Contributing

Contributions are welcome! Please ensure all tests pass before submitting pull requests.

## License

This project is maintained by Wanzhen Fu.

## Contact

For questions or support, please open an issue on the repository.

---

**Note**: This framework is designed for research and backtesting purposes. Always validate strategies with paper trading before deploying to live markets.