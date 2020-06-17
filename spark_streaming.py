import json
import os
import pickle

import utils
from Neo4j import Neo4j
from elastic import Elastic
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from mongo import MongoDB

COMPANIES = ['google', 'microsoft', 'ibm', 'sap', 'amazon', 'accenture', 'bmw', 'siemens', 'nvidia', 'apple']


def calculate_avg(data):
    """
    :param data: (list) of floats
    :return: (float) avg of data
    """
    sum = 0
    for value in data:
        sum += value

    return sum / len(data)


def create_spark_context(interval):
    """
    :param interval: in seconds. To create a local StreamingContext and batch interval of interval seconds
    :return:
    """

    sc = SparkContext("local[*]", "TwitterETL")
    ssc = StreamingContext(sc, interval)
    return ssc


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

def run_spark_job(ssc):
    """
    :param ssc: spark streaming context
    :return:
    """
    # get the tweets
    tweets = ssc.socketTextStream("localhost", 10001)

    # convert string data to json
    tweets_json = tweets.map(lambda x: json.loads(x))

    #load tweets to mongodb
    tweets_json.foreachRDD(lambda rdd: load_to_mongodb(rdd.collect()))

    # filter data based if hashtags are present in tweet or not
    filtered_tweets = tweets_json  # .filter(lambda x: len(x["entities"]["hashtags"]) > 0)

    # find out which companies are contained in the tweet
    company_tweet_pair = filtered_tweets.flatMap(lambda x: utils.identify_company(x, COMPANIES))

    # prune the the tweets
    pruned_tweets = company_tweet_pair.map(lambda x: utils.prune_tweet(x[1], x[0]))

    # cache it because it will be used more than once
    # pruned_tweets = pruned_tweets.cache()

    # stream analytics
    ssc.checkpoint("./checkpoints")

    tweet_window = pruned_tweets.window(windowDuration=300, slideDuration=5) \
        .map(lambda t: {"Company": t['company'], "Sentiment": round(t['sentiment'], 2)})
    tweet_window.foreachRDD(lambda rdd: load_to_elastic(rdd.collect()))

    # upload to neo4j
    pruned_tweets.foreachRDD(lambda rdd: load_to_neo4j(rdd.collect()))

    # pruned_tweets.pprint()


#if __name__ == "__main__":

def run_spark():
    try:
        ssc = create_spark_context(1)
        run_spark_job(ssc)

        # start the spark streaming context
        ssc.start()
        ssc.awaitTermination()

    finally:
        ssc.stop()
