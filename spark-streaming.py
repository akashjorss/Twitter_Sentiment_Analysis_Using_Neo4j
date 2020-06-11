from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pickle, json
import utils
from Neo4j import Neo4j

COMPANIES = ['google', 'microsoft', 'ibm', 'sap', 'amazon', 'accenture', 'bmw', 'siemens', 'nvidia', 'apple']
if __name__ == "__main__":
    # Create a local StreamingContext and batch interval of 1 second

    sc = SparkContext("local[*]", "TwitterETL")

    ssc = StreamingContext(sc, 1)

    # get the tweets
    tweets = ssc.socketTextStream("localhost", 10000)

    # convert string data to json
    tweets_json = tweets.map(lambda x: json.loads(x))

    # filter data based if hashtags are present in tweet or not
    filtered_tweets = tweets_json.filter(lambda x: x)#len(x["entities"]["hashtags"])>0)

    # find out which companies are contained in the tweet
    company_tweet_pair = filtered_tweets.flatMap(lambda x: utils.identify_company(x, COMPANIES))

    # prune the the tweets
    pruned_tweets = company_tweet_pair.map(lambda x: utils.prune_tweet(x[1], x[0]))

    # upload to neo4j
    #neo4j = Neo4j()
    #pruned_tweets.foreachRDD(lambda rdd: rdd.foreach(neo4j.load_data))

    pruned_tweets.pprint()

    #start the spark streaming context
    ssc.start()
    ssc.awaitTermination()




