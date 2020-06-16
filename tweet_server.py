import json
import os
import pickle
import socket
import sys
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka import KafkaProducer

consumer_key = os.environ['CONSUMER_KEY']
consumer_secret = os.environ['CONSUMER_SECRET']
access_token = os.environ['ACCESS_TOKEN']
access_secret = os.environ['ACCESS_SECRET']
COMPANIES = ['google', 'microsoft', 'ibm', 'sap', 'amazon', 'accenture', 'bmw', 'siemens', 'nvidia', 'apple']


class TwitterStream(StreamListener):

    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                 value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    def on_data(self, data):
        try:
            self.producer.send('twitter', value = data)
            print("The message is sent")
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))

        return True

    def on_error(self, status):
        print(status)
        return True


if __name__ == "__main__":

#def run_server():
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    twitter_stream = Stream(auth, TwitterStream())
    twitter_stream.filter(track=COMPANIES, languages=["en"])