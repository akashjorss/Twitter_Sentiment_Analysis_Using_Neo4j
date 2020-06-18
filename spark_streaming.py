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


def create_spark_context(interval):
    """
    :param interval: in seconds. To create a local StreamingContext and batch interval of interval seconds
    :return:
    """

    sc = SparkContext("local[*]", "TwitterETL")
    ssc = StreamingContext(sc, interval)
    return ssc


def run_spark_job(ssc):
    """
    :param ssc: spark streaming context
    :return:
    """
    # get the tweets
    tweets = ssc.socketTextStream("localhost", 10001)

    # convert string data to json
    tweets_json = tweets.map(lambda x: json.loads(x))

    # load tweets to mongodb
    # tweets_json.foreachRDD(lambda rdd: utils.load_to_mongodb(rdd.collect()))

    # find out which companies are contained in the tweet
    pruned_tweets = tweets_json.flatMap(lambda x: utils.identify_company(x, COMPANIES)) \
        .map(lambda x: utils.prune_tweet(x[1], x[0])).cache()

    # checkpointing necessary for window operations
    ssc.checkpoint("./checkpoints")

    pruned_tweets.window(windowDuration=300, slideDuration=5) \
        .map(lambda t: {"Company": t['company'], "Sentiment": round(t['sentiment'], 2)}) \
        .foreachRDD(lambda rdd: utils.load_to_elastic(rdd.collect()))

    # only load tweets which have hashtag to neo4j and which have non zero sentiment
    pruned_tweets.filter(lambda x: len(x["hashtags"]) > 0)\
        .filter(lambda x: not(x["sentiment"] == 0.0)) \
        .foreachRDD(lambda rdd: utils.load_to_neo4j(rdd.collect()))


def run_spark():
    try:
        ssc = create_spark_context(1)
        run_spark_job(ssc)

        # start the spark streaming context
        ssc.start()
        ssc.awaitTermination()

    finally:
        ssc.stop()
