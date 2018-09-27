import pymongo
from const import DB_HOST, DB_PORT

# 创建mongodb的连接对象
client = pymongo.MongoClient(host=DB_HOST, port=DB_PORT)

# 指定数据库
db = client.test

# 指定集合
collection = db.spider_rent


# 创建唯一索引
# db.spider_rent.create_index('age', unique=True)


def insert_into_db(record):
    if type(record) == list:
        data = collection.insert_many(record)
    elif type(record) == dict:
        data = collection.insert_one(record)
    else:
        print('wrong record format')
        data = 0
    if not data:
        print('inserting failed')


def select_one_from_db(condition=None):
    data = collection.find_one(condition)
    return data


def select_many_from_db(condition=None):
    data = collection.find(condition)
    return data


def select_many_and_sort_db(feature, condition=None, order=pymongo.ASCENDING):
    data = collection.find(condition).sort(feature, order)
    return data


if __name__ == "__main__":
    # record = [{'age': {'guess':20}}, {'type': 'y'}]
    # insert_into_db(record)
    results = select_many_and_sort_db('addr')
    print(results.count())
    for result in results:
        print(result)
