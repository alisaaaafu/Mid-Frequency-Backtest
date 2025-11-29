# encoding=utf-8
"""
Author: Wamnzhen Fu
Date: 7-7-2020
"""

from abc import ABCMeta,abstractmethod
import pymongo
from pymongo.errors import ConnectionFailure,OperationFailure
from Utils.DataStructure import MONGODATA

class DataHandlerBase(object):

    __metaclass__ = ABCMeta


class MongoDBHandler(DataHandlerBase):
    """
    市场数据DB:不同品种的cols
    Order DB
    Account DB
    Position DB
    """
    def __init__(self, config):
        self.config = config
        self.host = self.config["DB"]["Mongo_Host"]
        self.port = int(self.config["DB"]["Mongo_Port"])
        self.client = None
        self.__connection = False

    def Connect_DB(self):
        """
        链接MongoDB
        :return:
        """
        try:
            if not self.client and not self.__connection:
                self.client = pymongo.MongoClient(host=self.host,
                                                  port=self.port)
                # 检查链接状态
                self.client.server_info()
                self.__connection = True
        except:
            raise ConnectionFailure("链接Mongo数据库失败")

    def on_collections_query(self,db:MONGODATA):
        """
        获取db下集合列表
        """
        try:
            db = self.client[db.DB]
            col_names = list(db.list_collection_names())

            return col_names
        except:
            raise OperationFailure("获取DB集合列表失败")

    def build_in_generator(self,db:MONGODATA):
        """
        将指定的db下的col变成生成器
        :return:
        """
        my_db = self.client[db.DB]
        my_col = my_db[db.COL]
        db.Data = my_col.find(no_cursor_timeout=True)

    def on_insert(self,db:MONGODATA):
        """
        插入数据
        :return:
        """
        try:
            my_db = self.client[db.DB]
            my_col = my_db[db.COL]

            data = db.Info['data']

            my_col.insert_one(data)
            return True
        except:
            raise OperationFailure("DB插入单条数据失败")

    def on_insert_many(self,db:MONGODATA):
        """
        插入多条数据
        """
        try:
            my_db = self.client[db.DB]
            my_col = my_db[db.COL]

            data = db.Info['data']

            if len(data) == 0:
                return

            my_col.insert_many(data)
            return True
        except:
            raise OperationFailure("DB插入多条数据失败")

    def on_query_one(self,db:MONGODATA):
        """
        查询符合条件的第一条数据
        :return:
        """
        try:
            my_db = self.client[db.DB]
            my_col = my_db[db.COL]

            query = db.Info['req']    ### query可以为空, {}形式

            result = my_col.find_one(query)
            return result               ### 返回的是dict格式
        except:
            raise OperationFailure("DB查询单条数据失败")

    def on_query_many(self,db:MONGODATA):
        """
        查询多条数据
        """
        try:
            my_db = self.client[db.DB]
            my_col = my_db[db.COL]

            query = db.Info['req']

            result = my_col.find(query)
            return list(result)           ### 将Cursor类型转换成list,list中每一个元素是dict类型
        except:
            raise OperationFailure("DB查询多条数据失败")

    def on_find_and_replace_one(self,db:MONGODATA):
        """
        替换DB中的一个元素
        """
        try:
            my_db = self.client[db.DB]
            my_col = my_db[db.COL]

            query = db.Info['req']

            target = my_col.find_one(query)

            my_col.replace_one(target,db.Info['data'],True)
            return True

        except:
            raise OperationFailure("DB查找并替换元素失败")

    def on_update_one(self,db:MONGODATA):
        """
        更新匹配的第一个元素
        Info['req']包含两部分, preQuery:{}和newQuery:{}
        """
        try:
            my_db = self.client[db.DB]
            my_col = my_db[db.COL]

            preQuery = db.Info['req']['preQuery']
            newQuery = db.Info['req']['newQuery']

            my_col.update_one(preQuery,newQuery)
            return True
        except:
            raise OperationFailure("DB更新单个元素失败")

    def on_update_many(self,db:MONGODATA):
        """
        更新所有匹配的元素
        """
        try:
            my_db = self.client[db.DB]
            my_col = my_db[db.COL]

            preQuery = db.Info['req']['preQuery']
            newQuery = db.Info['req']['newQuery']

            my_col.update_many(preQuery,newQuery)
            return True
        except:
            raise OperationFailure("DB更新多个元素失败")

    def on_delete_one(self,db:MONGODATA):
        """
        删除匹配的第一条数据
        :return:
        """
        try:
            my_db = self.client[db.DB]
            my_col = my_db[db.COL]

            query = db.Info['req']

            my_col.delete_one(query)
            return True
        except:
            raise OperationFailure("DB删除一个元素失败")

    def on_delete_many(self,db:MONGODATA):
        """
        删除所有匹配的数据
        """
        try:
            my_db = self.client[db.DB]
            my_col = my_db[db.COL]

            query = db.Info['req']

            my_col.delete_many(query)
            return True
        except:
            raise OperationFailure("DB删除多个元素失败")

    def on_delete_collection(self,db:MONGODATA):
        """
        删除集合
        """
        try:
            my_db = self.client[db.DB]
            my_col = my_db[db.COL]

            re = my_col.drop()
            if re:
                print("删除col成功")
            else:
                print("删除col失败")
        except:
            raise OperationFailure("DB删除collections失败")

    def on_sort(self,db:MONGODATA,ascending=1):
        """
        排序
        """
        try:
            my_db = self.client[db.DB]
            my_col = my_db[db.COL]
            target_var = db.Info['req']

            sortedData = my_col.find().sort(target_var,ascending)
            return sortedData
        except:
            raise OperationFailure("DB数据排序失败")


    def disconnected(self):
        """
        关闭与数据库的链接
        """
        self.__connection = False
        if self.client:
            self.client.close()
        self.client = None


class CSVHandler(DataHandlerBase):
    def __init__(self):
        """
        not implemented
        """
        pass

    def insert_data(self,data):
        """
        not implemented
        :param data:
        :return:
        """
        pass

    def delete_data(self,data):
        """
        not implemented
        :param data:
        :return:
        """
        pass

    def replace_data(self,data):
        """
        not implemented
        :param data:
        :return:
        """
        pass

    def query_data(self,data):
        """
        not implemented
        :param data:
        :return:
        """
        pass

    def update_data(self):
        pass


class RealTimeDataHandler(DataHandlerBase):
    def __init__(self):
        pass

    def websocket(self):
        """
        websocket读取数据
        """
        pass

    def save(self):
        """
        保存数据
        """
        pass

    def update_data(self):
        """
        更新本地的数据
        """
        pass

    def query_data(self):
        """
        查询本地的数据
        """
        pass

    def delete_date(self):
        """
        删除本地数据
        """
        pass

    def replace_data(self):
        """
        替换数据
        """
        pass

    def insert_data(self):
        """
        插入数据
        """
        pass


if __name__ == "__main__":
    mongo_service = MongoDBHandler()
    mongo_service.Connect_DB()

    data = MONGODATA(DB="test",COL="testCol",Info={"data":{"timestamp":"000000004",'bids': [[1, 2], [3, 2]],'asks': [[3, 1], [4, 6]]},"req":{"timestamp":"000000004"}},Data=None)

    re = mongo_service.on_query_one(data)
    print(re)

    #### 测试通过
