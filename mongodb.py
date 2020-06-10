import json

from pymongo import MongoClient


def InsertOne(file):
    client = MongoClient('localhost', 27017)
    db = client['test']
    collection = db['tweets']
    count = 0

    with open(file, 'r') as f:
        for line in f:
            tweet = json.loads(line)
            collection.insert_one(tweet)
            count += 1

    print("Insert: " + str(file) + "\nTotal " + str(count) + " documents had inserted successfully!")

    client.close()


def BulkLoad(files):
    client = MongoClient('localhost', 27017)
    db = client['test']
    collection = db['tweets']
    count = 0

    for file in files:
        with open(file) as f:
            for line in f:
                tweet = json.loads(line)
                collection.insert_one(tweet)
                count += 1

    print("BuckLoad: " + str(files) + "\nTotal " + str(count) + " documents had inserted successfully!")
    client.close()


def DeleteAllTweets():  # e.g. one company
    client = MongoClient('localhost', 27017)
    db = client['test']
    collection = db['tweets']

    x = collection.delete_many({})
    print(x.deleted_count, "documents deleted")

    client.close()


def DeleteTweet(query):
    client = MongoClient('localhost', 27017)
    db = client['test']
    collection = db['tweets']
    x = collection.delete_many(query)
    print(x.deleted_count, " documents deleted.")


def ShowTweets():
    client = MongoClient('localhost', 27017)
    db = client['test']
    collection = db['tweets']
    cursor = collection.find()
    for document in cursor:
        print(document)
    client.close()

# InsertOne('tweets.json')
# DeleteAllTweets()
# BulkLoad(['tweets.json', 'tweets2.json', 'tweets3.json'])
# DeleteTweet({"text": {"$regex": 'RT .*'}})
# ShowTweets()
