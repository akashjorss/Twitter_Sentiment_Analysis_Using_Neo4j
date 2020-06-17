import json
from pymongo import MongoClient


class MongoDB:
    def __init__(self):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['MindMiner']
        self.collection = self.db['tweets']

    def insert_doc(self, doc):
        """
        :param doc: a json document
        :return: True on sucess, False on error
        """
        try:
            self.collection.insert_one(doc)
            print(
                "Document inserted successfully to db " + self.db.__str__() + " and to collection " + self.collection.__str__())
            return True
        except:
            print("Document not inserted")
            return False

    def bulk_load(self, docs):
        """
        :param docs: list of documents
        :return: None
        """
        for doc in docs:
            self.insert_doc(doc)

    def clear_collection(self):
        self.collection.delete_many({})

    def delete_by_query(self, query):
        x = self.collection.delete_many(query)
        print(x.deleted_count, " documents deleted.")

    def show_docs(self):
        cursor = self.collection.find().limit(10)
        print("Displaying first 10 docs in database", self.db.name, "and in collection", self.collection.name)
        for document in cursor:
            print(document)

    def __del__(self):
        self.client.close()


if __name__ == "__main__":
    # instantiate mongod instance
    mongodb = MongoDB()
    # load some tweets in the db
    # with open('./Artifacts/tweets.json', 'r') as f:
    #     tweets = f.readlines()
    # for tweet in tweets:
    #     mongodb.insert_doc(json.loads(tweet))
    # display some tweets
    mongodb.show_docs()
    # clear the collection
   # mongodb.clear_collection()
