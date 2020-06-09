import os
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json


class MyListener(StreamListener):

    def on_data(self, data):
        try:
            with open('huawei_tweets.json', 'a') as f:
                f.write(data)
                print(json.loads(data)["text"])
                return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


consumer_key = os.environ['CONSUMER_KEY'] #"Damib7oklPRnO1ylqzb0Yd0I6"
consumer_secret = os.environ['CONSUMER_SECRET'] #"KRP00SvjGAn0WYpMC8IPmbbl6jYvVMKkAbujjPrVE1OJADY3yB"
access_token = os.environ['ACCESS_TOKEN'] #"1119247282388344832-8d30pPeXuRpxLUr0Ps5jk3UhJEEC1N"
access_secret = os.environ['ACCESS_SECRET'] #"8WEBOgqe1417FmMpZwCwurlWqYooqwmn9oWvuyFlZKw8e"

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

twitter_stream = Stream(auth, MyListener())
twitter_stream.filter(track=['huawei'], languages=["en"])
