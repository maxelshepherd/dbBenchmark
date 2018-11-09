import pymongo
from pymongo import MongoClient
import time
from multiprocessing import Pool
from cassandra.cluster import Cluster
from cassandra import ReadTimeout
import threading
from itertools import product
import itertools
from functools import partial
from multiprocessing import Process, Value, Lock
from tables import *


# print('init mongoDB...')
# client = MongoClient()
# client = MongoClient('localhost', 27017)
# db = client['70101200027_small']
# data = []
#
# def query(name):
#     cpt = 0
#     collection = db[name]
#     animals = collection.find_one()["animals"]
#     for animal in animals:
#         tag_data = animal["tag_data"]
#         for entry in tag_data:
#             cpt += 1
#             # data.append(entry)
#     return cpt
#
#
# if __name__ == "__main__":
#     cont = ""
#     colNames = db.list_collection_names()
#     start_time = time.time()
#     pool = Pool(processes=4)
#     result = pool.map(query, colNames)
#     pool.close()
#     elapsed_time = time.time() - start_time
#     print(elapsed_time)
#     print("in secondes.")
#     print(sum(result))
#     print(cont)

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
#     future = session.execute_async("SELECT * FROM \"%s\"" % table)
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
#     result = pool.map(query, tables)
#     pool.close()
#     elapsed_time = time.time() - start_time
#     print(elapsed_time)
#     print("in secondes.")
#     print(sum(result))



######################################################################################

print("pytable benckmark...")
cpt = 0;
file_name = "C:\SouthAfrica\data_benchmark.h5"
h5file = open_file(file_name, mode="a", title="database file")
group = h5file.root._70101200027_small
start_time = time.time()
for table in group:
    for x in table.iterrows():
        cpt += 1
        data = [x['date'], x['time'], x['first_sensor_value'], x['second_sensor_value'], x['serial_number'], x['signal_strength']]
        # print(data)
elapsed_time = time.time() - start_time
print(elapsed_time)
print("in secondes.")
print(cpt)

