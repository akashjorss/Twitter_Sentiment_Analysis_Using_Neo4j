from py2neo import Graph
from py2neo import Graph, Node, Relationship
import json
from textblob import TextBlob

def load_data(tweet):
    """
    :param tweet: a json doc with following schema
    {
        "type": "record",
        "name": "tweet",
        "keys" : [
            {"name": "company", "type": "string"},
            {"name": "sentiment", "type": "integer"},
            {"name": "id", "type": "string"},
            {"name": "date", "type": "string"},
            {"name": "time", "type": "string"},
            {"name": "retweet_count", "type": "integer"}
            {"name":"hashtags", "type":array}
            ]
    }
    :return: None
    """
    graph = Graph("bolt://localhost:7687", auth = ("neo4j", "password"), database = "Database")
    tx = graph.begin()
    company_node = Node("Company", name=tweet["company"])
    tx.create(company_node)
    tweet_node = Node("Tweet", id = tweet["id"], sentiment=tweet["sentiment"], retweet_count = tweet["retweet_count"])#specify properties here)
    tx.create(tweet_node)
    # create datetime node with discretisation
    datetime_node = Node("DateTime", name=tweet["time"].split(":")[0]+"_"+tweet["date"])
    if graph.exists(datetime_node) == False:
        print("False")
        tx.create(datetime_node)
    created_on = Relationship.type("CREATED_ON")
    tx.merge(created_on(tweet_node, datetime_node), "Tweet", "name")
    # create hashtag nodes and check their existence
    # if they exist form relationship with them else create them
    for hashtag in tweet["hashtags"]:
        hashtag_node = Node("Hashtag", name=hashtag)
        has_hashtag = Relationship.type("HAS_HASHTAG")
        tx.merge(has_hashtag(tweet_node, hashtag_node), "Tweet", "name")
        # if graph.exists(hashtag_node):
        #     pass #form relationship between tweet and hashtag
        # else:
        #     tx.create(hashtag_node)

    tx.commit()


def prune_tweet(tweet, company):
    """
    :param tweet: a native tweet object
    :return: tweet, a modified tweet object with only relevant keys
    """
    #calculate polarity
    polarity = TextBlob(tweet["text"]).sentiment.polarity
    hashtags = []
    for h in tweet["entities"]["hashtags"]:
        hashtags.append(h["text"])
    date = tweet["created_at"].split(" ")[2]+"_"+tweet["created_at"].split(" ")[1]+"_"+tweet["created_at"].split(" ")[5]
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


# read google tweets file
with open('Artifacts/google_tweets.json', 'r') as f:
    tweets = f.readlines()

# clear the graph
graph = Graph("bolt://localhost:7687", auth = ("neo4j", "password"), database = "Database")
graph.delete_all()

# load the data in graph
for tweet in tweets:
    load_data(prune_tweet(json.loads(tweet), 'google'))