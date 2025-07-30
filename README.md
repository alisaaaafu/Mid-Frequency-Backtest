# README #

This is a medium-frequency backtest engine.

### Cosmos Systematics Backtest System ###

* This is designed for medium-frequency strategy backtest.
* V-1.1.0

### Framework ###

* DataBase & Feeder
* Order Matching Algo
* Exchanges
* Strategy Base

### Quick Start ###
- Install necessary packages
`pip install -r requirements.txt`
- Install MongoDB locally
See the following instruction `https://www.mongodb.com/zh-cn/docs/manual/administration/install-community/`
- (Optional) Install DataGrip, a cross-platform database IDE
`https://www.jetbrains.com/datagrip/download/#section=mac`

#### Strategy Backtest 
- New startegy script under `Strategy` folder, `./Strategy/YOUR_STRATEGY.py`
- In `config.json`, add your strategy configuration with the key `YOUR_STRATEGY`.
- Run `python run_strategy.py -c YOUR_CONFIG`, backtest results will be stored in folder `./bt_result/user/`

#### Data Path on Cosmos Server
- klines data: `/srv/data/BinanceU/klines/1m/{symbol}/BinanceU_{symbol}_perp.parquet`
- funding data: `/srv/data/BinanceU/funding/Funding_BinanceU_{symbol}_perp.parquet`

- Current Supported Cryptos
    - BTCUSDT, BTCUSDC, ETHUSDT, ETHUSDC, BNBUSDT, BNBUSDC, SOLUSDT, SOLUSDC

#### Factor Miner & Analyzer
- Support deap to mine factors based on basic factors.
- Analyze correlation between future return and factors, and intraday patterns of factors.