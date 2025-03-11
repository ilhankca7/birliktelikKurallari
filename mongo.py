# mongo.py

from pymongo import MongoClient, errors

class Mongo:
    def __init__(self, dsn=None, dbname=None):
        self.dsn = dsn or "mongodb://localhost:27017"
        self.dbname = dbname or "smartydata"
        self.client = None
        self.db = None
        self.mongoConnect()

    def mongoConnect(self):
        try:
            self.client = MongoClient(self.dsn)
            self.db = self.client[self.dbname]
            print("MongoDB'ye başarılı bir şekilde bağlanıldı.")
        except errors.ConnectionFailure as e:
            print(f"MongoDB bağlantı hatası: {e}")
            self.client = None
            self.db = None

    def insertOne(self, collection, document):
        try:
            result = self.db[collection].insert_one(document)
            return result.inserted_id
        except errors.PyMongoError as e:
            print(f"MongoDB insertOne hatası: {e}")
            return None

    def insertMany(self, collection, documents):
        try:
            result = self.db[collection].insert_many(documents)
            return result.inserted_ids
        except errors.PyMongoError as e:
            print(f"MongoDB insertMany hatası: {e}")
            return None

    def findOne(self, collection, query, projection=None, sort=None):
        try:
            return self.db[collection].find_one(query, projection, sort)
        except errors.PyMongoError as e:
            print(f"MongoDB findOne hatası: {e}")
            return None

    def find(self, collection, query, projection=None, sort=None, limit=None, skip=None):
        try:
            cursor = self.db[collection].find(query, projection)
            if sort:
                cursor = cursor.sort(sort)
            if skip:
                cursor = cursor.skip(skip)
            if limit:
                cursor = cursor.limit(limit)
            return list(cursor)
        except errors.PyMongoError as e:
            print(f"MongoDB find hatası: {e}")
            return None

    def updateOne(self, collection, query, update, upsert=False):
        try:
            return self.db[collection].update_one(query, update, upsert=upsert)
        except errors.PyMongoError as e:
            print(f"MongoDB updateOne hatası: {e}")
            return None

    def updateMany(self, collection, query, update, upsert=False):
        try:
            return self.db[collection].update_many(query, update, upsert=upsert)
        except errors.PyMongoError as e:
            print(f"MongoDB updateMany hatası: {e}")
            return None

    def deleteOne(self, collection, query):
        try:
            return self.db[collection].delete_one(query)
        except errors.PyMongoError as e:
            print(f"MongoDB deleteOne hatası: {e}")
            return None

    def deleteMany(self, collection, query):
        try:
            return self.db[collection].delete_many(query)
        except errors.PyMongoError as e:
            print(f"MongoDB deleteMany hatası: {e}")
            return None

    def mongoClose(self):
        if self.client:
            self.client.close()
            print("MongoDB bağlantısı kapatıldı.")
