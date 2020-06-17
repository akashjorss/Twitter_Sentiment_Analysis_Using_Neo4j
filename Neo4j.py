from py2neo import Graph, Node, NodeMatcher, Relationship
import json
import utils
import sys


class Neo4j:
    def __init__(self):
        # initialize the self.graph
        self.graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"), database="twitter")
        self.matcher = NodeMatcher(self.graph)

    def delete_all(self):
        self.graph.delete_all()

    def load_data(self, tweet):
        """
        Loads one tweet at a time
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
        # begin transaction
        tx = self.graph.begin()

        # retrieve company node from the remote self.graph
        company = self.graph.evaluate("MATCH(n) WHERE n.name = {company} return n", company=tweet["company"])
        # if remote node is null, create company node
        if company is None:
            company = Node("Company", name=tweet["company"])
            self.graph.create(company)
            # print("Node created:", company)

        # repeat above for all nodes
        tweet_node = self.graph.evaluate("MATCH(n) WHERE n.id = {id} return n", id=tweet["id"])
        if tweet_node is None:
            tweet_node = Node("Tweet", id=tweet["id"], sentiment=tweet["sentiment"], retweet_count=tweet["retweet_count"])
            tx.create(tweet_node)
            # print("Node created:", tweet_node)

        datetime = self.graph.evaluate("MATCH(n) WHERE n.time = {time} AND n.date = {date} return n",
                                  time=tweet["time"].split(":")[0]+':'+tweet["time"].split(':')[1],
                                  date=tweet["date"])
        if datetime is None:
            datetime = Node("DateTime", time=tweet["time"].split(":")[0]+':'+tweet["time"].split(":")[1], date=tweet["date"])
            # tx.create(datetime)
            # print("Node created:", datetime)

        # create relationships
        # check if describes already exists
        # describes = Relationship(tweet_node, "DESCRIBES", company)
        # created_on = Relationship(tweet_node, "CREATED_ON", datetime)
        # tx.create(describes)
        # tx.create(created_on)
        # print("Relationships created")

        # create hashtag nodes and connect them with tweet nodes
        for hashtag in tweet["hashtags"]:
            hashtag_node = self.matcher.match("Hashtag", name=hashtag).first()
            # hashtag_node = self.graph.evaluate("MATCH(n) WHERE n.name = {hashtag} return n", hashtag=hashtag)
            if hashtag_node is None:
                hashtag_node = Node("Hashtag", name=hashtag)
                tx.create(hashtag_node)
                about = Relationship(hashtag_node, "ABOUT", company)
                tx.create(about)

            contains_hashtag = Relationship(tweet_node, "CONTAINS", hashtag_node)
            tx.create(contains_hashtag)

        # commit transaction
        tx.commit()


    def bulk_load(self, tweets):
        """
        Bulk loads list of tweets
        :param self:
        :param tweets:
        :return:
        """
        for t in tweets:
            self.load_data(t)
            print("Tweet loaded into neo4j")

    def prune_graph(self):
        self.graph.evaluate('MATCH (t:Tweet)-[:CONTAINS]->(n) WITH n as n, count(t) as tweet_count WHERE tweet_count < 2 DETACH DELETE n')
        print('Graph pruned!')


if __name__ == "__main__":
    # read tweets files of different companies
    with open('Artifacts/google_tweets.json', 'r') as f:
        google_tweets = f.readlines()
    with open('Artifacts/apple_tweets.json', 'r') as f:
        apple_tweets = f.readlines()
    with open('Artifacts/apple_tweets.json', 'r') as f:
        huawei_tweets = f.readlines()

    # initialize the graph
    neo4j = Neo4j()

    # clear the self.graph
    neo4j.delete_all()

    # load the data in self.graph
    # for tweet in google_tweets:
    #     # discard the tweets which don't have hashtag
    #     tweet_json = json.loads(tweet)
    #     print(sys.getsizeof(tweet_json))
    #     if len(tweet_json["entities"]["hashtags"]) != 0:
    #         neo4j.load_data(utils.prune_tweet(tweet_json, 'google'))
    #
    # for tweet in apple_tweets:
    #     # discard the tweets which don't have hashtag
    #     tweet_json = json.loads(tweet)
    #     if len(tweet_json["entities"]["hashtags"]) != 0:
    #         neo4j.load_data(utils.prune_tweet(tweet_json, 'apple'))

    for tweet in huawei_tweets:
        # discard the tweets which don't have hashtag
        tweet_json = json.loads(tweet)
        if len(tweet_json["entities"]["hashtags"]) != 0:
            neo4j.load_data(utils.prune_tweet(tweet_json, 'huawei'))

    neo4j.prune_graph()