import pymongo
from pymongo import MongoClient
import time
from multiprocessing import Pool
from cassandra.cluster import Cluster

print('init mongoDB...')
client = MongoClient()
client = MongoClient('localhost', 27017)
db = client['70101200027']
data = []


def query(name):
    collection = db[name]
    animals = collection.find_one()["animals"]
    for animal in animals:
        tag_data = animal["tag_data"]
        # for entry in tag_data:
        #     data.append(entry)
    return 0


if __name__ == "__main__":
    colNames = db.list_collection_names()
    start_time = time.time()
    pool = Pool(processes=4)
    result = pool.map(query, colNames[0:1])
    pool.close()
    elapsed_time = time.time() - start_time
    print(elapsed_time)
    print("in secondes.")
    print(result)
