from textblob import TextBlob
from Neo4j import Neo4j
from elastic import Elastic
from mongo import MongoDB
import os


def prune_tweet(tweet, company):
    """
    :param tweet: a json tweet object
    :param company: company name
    :return: tweet, a modified tweet object with only relevant keys
    """
    # calculate polarity
    polarity = TextBlob(tweet["text"]).sentiment.polarity
    hashtags = []
    for h in tweet["entities"]["hashtags"]:
        hashtags.append('#' + h["text"])
    date = tweet["created_at"].split(" ")[2] + "_" + tweet["created_at"].split(" ")[1] + "_" + \
           tweet["created_at"].split(" ")[5]
    time = tweet["created_at"].split(" ")[3]
    modified_tweet = {
        "id": tweet["id_str"],
        "company": company,
        "sentiment": polarity,
        "retweet_count": tweet["retweet_count"],
        "date": date,
        "time": time,
        "hashtags": hashtags,
    }
    return modified_tweet


def identify_company(tweet, companies):
    """
    :param tweet: tweet object
    :param companies: list of companies
    :return: list of (company, tweet) pairs
    """
    flatmap = []
    for company in companies:
        if company in tweet['text']:
            flatmap.append((company, tweet))

    return flatmap


def load_to_elastic(json_docs):
    """
    :param json_docs: list of json docs
    :return: None
    """

    # upload to elastic
    if len(json_docs) > 0:
        print(json_docs)
        elastic = Elastic(cloud_id=os.environ['ELASTIC_CLOUD_ID'],
                          username=os.environ['ELASTIC_USERNAME'], password=os.environ['ELASTIC_PASSWORD'])
        elastic.clear_data("tweet-index")
        elastic.load_data(json_docs, "tweet-index")


def load_to_neo4j(tweets):
    """
    :param tweets: list of json object
    :return: None
    """
    neo4j = Neo4j()
    neo4j.bulk_load(tweets)


def load_to_mongodb(tweets):
    mongodb = MongoDB()
    mongodb.bulk_load(tweets)
