# encoding=utf-8
"""
Author:Wamnzhen Fu
Date:7-10-2020
"""

import pymongo
import csv
import time
import pandas as pd
from datetime import datetime, timedelta


def connect_mongo(db_, col):
    # mongo_uri = 'mongodb://user_name:password@host:port/验证数据库'
    client = pymongo.MongoClient("localhost:27017")
    db = client[db_]
    exits_cols = list(db.list_collection_names())
    if col in exits_cols:
        raise KeyError
    else:
        collection = db[col]
        return collection


def insertToMongoDB(sets, csv_file):
    # df = pd.read_csv(csv_file, encoding='utf-8')
    # dt = list()
    # for item in df['timestamp']:
    #     utc_date = datetime.strptime(item, "%Y-%m-%dT%H:%M:%S.%fZ")
    #     local_date = utc_date + timedelta(hours=8)
    #     print(datetime.strftime(local_date, '%Y-%m-%d %H:%M:%S'))
    #     dt.append(datetime.strftime(local_date, '%Y-%m-%d %H:%M:%S'))
    #
    # df['timestamp'] = dt
    # df.to_csv(csv_file)

    with open(csv_file, 'r', encoding='utf-8') as csvfile:
        # 调用csv中的DictReader函数直接获取数据为字典形式
        reader = csv.DictReader(csvfile)
        csv_data = []
        counts = 0
        index = 1
        for each in reader:
            csv_data.append(each)
            if index == 10000:  # 10000个之后写入MongoDB中
                sets.insert_many(csv_data)
                csv_data.clear()
                index = 0
                print("成功添加了" + str(counts) + "条数据")
            counts += 1
            index += 1
        if len(csv_data) > 0:  # 剩余的数据
            sets.insert_many(csv_data)
            print("成功添加了%s条数据" % len(csv_data))


if __name__ == '__main__':
    print(time.strftime('%Y-%m-%d %H:%M:%S'))  # 计算时间用

    database_mk = f'MarketData'
    collection = [
                f'Funding_BinanceU_BTCUSDC_perp',
                'BinanceU_BTCUSDC_perp'
                  ]

    for col in collection:
        mk_csv_file = f'./{col}.csv'
        set_mk = connect_mongo(database_mk, col)
        insertToMongoDB(set_mk, mk_csv_file)

        # td_csv_file = f'xxxxxxxx.csv'
        #
        # pre_csv = str(data_time) + '/' + td_csv_file[11:22]+'trade-'+td_csv_file[22:]
        #
        # tmp = pd.read_csv(pre_csv,header=None)
        # tmp.columns = ['time', 'symbol', 'price', 'direction', 'volume', 'timestamp', 'time2', 'timestamp2']
        # tmp = tmp.drop(columns=['time2', 'timestamp2'])
        # tmp['timestamp'] = list(map(lambda x: int(1000 * float(x)), tmp['timestamp']))
        # tmp.to_csv(td_csv_file, index=False)
        #
        # set_td = connect_mongo(database_td, col)
        # insertToMongoDB(set_td, td_csv_file)

    print(time.strftime('%Y-%m-%d %H:%M:%S'))
