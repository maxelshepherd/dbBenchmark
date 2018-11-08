import pymongo
from pymongo import MongoClient
import time
from multiprocessing import Pool
from cassandra.cluster import Cluster
from cassandra import ReadTimeout

# print('init mongoDB...')
# client = MongoClient()
# client = MongoClient('localhost', 27017)
# db = client['70101200027']
# data = []
#
#
# def query(name):
#     collection = db[name]
#     animals = collection.find_one()["animals"]
#     for animal in animals:
#         tag_data = animal["tag_data"]
#         # for entry in tag_data:
#         #     data.append(entry)
#     return 0
#
#
# if __name__ == "__main__":
#     colNames = db.list_collection_names()
#     start_time = time.time()
#     pool = Pool(processes=4)
#     result = pool.map(query, colNames[0:1])
#     pool.close()
#     elapsed_time = time.time() - start_time
#     print(elapsed_time)
#     print("in secondes.")
#     print(result)

######################################################################################
# cluster = Cluster(['127.0.0.1'])
# farm_id = "70101200027"
# session = cluster.connect(farm_id)
# session.set_keyspace(farm_id)
# meta = cluster.metadata.keyspaces[farm_id]
# tables = list(meta.tables.keys())
#
#
# def query(table):
#     cpt = 0
#     future = session.execute_async("SELECT date, time, first_sensor_value FROM \"%s\"" % table)
#     try:
#         rows = future.result()
#         for entry in rows:
#             cpt += 1
#         # for entry in rows:
#         #     print(entry.time, entry.date, entry.first_sensor_value)
#     except ReadTimeout:
#         print("Query timed out:")
#     return cpt
#
#
# if __name__ == "__main__":
#     print("cassandra benckmark...")
#     start_time = time.time()
#     pool = Pool(processes=4)
#     s = int(len(tables)/4)
#     result = pool.map(query, tables[0:s])
#     pool.close()
#     elapsed_time = time.time() - start_time
#     print(elapsed_time)
#     print("in secondes.")
#     print(sum(result))



######################################################################################