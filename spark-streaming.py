from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pickle, json
import utils
from Neo4j import Neo4j
from elastic import Elastic
import os

COMPANIES = ['google', 'microsoft', 'ibm', 'sap', 'amazon', 'accenture', 'bmw', 'siemens', 'nvidia', 'apple']

def create_spark_context(interval):
    """
    :param interval: in seconds. To create a local StreamingContext and batch interval of interval seconds
    :return:
    """

    sc = SparkContext("local[*]", "TwitterETL")
    ssc = StreamingContext(sc, interval)
    return ssc

def load_to_elastic(tweets):
    """
    :param tweets: list of json objects
    :return: None
    """
    # upload to elastic
    # elastic = Elastic(cloud_id=os.environ['ELASTIC_CLOUD_ID'],
    #                   username=os.environ['ELASTIC_USERNAME'], password=os.environ['ELASTIC_PASSWORD'])
    # elastic.load_data(tweets, "tweet-index")
    print("Here are the tweets for elastic")
    print(tweets)


def load_to_neo4j(tweets):
    """
    :param tweets: list of json object
    :return: None
    """
    neo4j = Neo4j()
    neo4j.bulk_load(tweets)


def run_spark_job(ssc):
    """
    :param ssc: spark streaming context
    :return:
    """
    # get the tweets
    tweets = ssc.socketTextStream("localhost", 10001)
    # convert string data to json
    tweets_json = tweets.map(lambda x: json.loads(x))

    # filter data based if hashtags are present in tweet or not
    filtered_tweets = tweets_json #.filter(lambda x: len(x["entities"]["hashtags"]) > 0)

    # find out which companies are contained in the tweet
    company_tweet_pair = filtered_tweets.flatMap(lambda x: utils.identify_company(x, COMPANIES))

    # prune the the tweets
    pruned_tweets = company_tweet_pair.map(lambda x: utils.prune_tweet(x[1], x[0]))

    # cache it because it will be used more than once
    # pruned_tweets = pruned_tweets.cache()

    # stream analytics
    ssc.checkpoint("./checkpoints")
    stream_analytics = pruned_tweets.map(lambda t: (t['company'], 1)) \
                                     .reduceByKeyAndWindow(func=lambda x, y: x+y, invFunc=lambda x, y: x-y,
                                                          windowDuration=300, slideDuration=5)

    stream_analytics.foreachRDD(lambda rdd: load_to_elastic(dict({"company": t[0], "count": t[1]}) for t in rdd.collect())) #generating wierd result

    # upload to neo4j
    pruned_tweets.foreachRDD(lambda rdd: load_to_neo4j(rdd.collect()))

    #pruned_tweets.pprint()


if __name__ == "__main__":

    try:
        ssc = create_spark_context(1)
        run_spark_job(ssc)

        #
        #
        # #start the spark streaming context
        ssc.start()
        ssc.awaitTermination()

    finally:
        ssc.stop()



