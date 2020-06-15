import json

from pymongo import MongoClient


def InsertTweet(filenmae):

    client = MongoClient('localhost', 27017)
    db = client['test']
    collection = db['tweets']

    with open(filenmae, 'r') as f:
        for line in f:
            tweet = json.loads(line)
            collection.insert_one(tweet)

    client.close()

InsertTweet('tweets.json')
