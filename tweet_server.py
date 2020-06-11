import os
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
import socket
import pickle
import sys

consumer_key = os.environ['CONSUMER_KEY']
consumer_secret = os.environ['CONSUMER_SECRET']
access_token = os.environ['ACCESS_TOKEN']
access_secret = os.environ['ACCESS_SECRET']
COMPANIES = ['google', 'microsoft', 'ibm', 'sap', 'amazon', 'accenture', 'bmw', 'siemens', 'nvidia', 'apple']

class TwitterStream(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            self.client_socket.send(data.encode('utf-8'))
            print("The message is sent")
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))

        return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket):

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    twitter_stream = Stream(auth, TwitterStream(c_socket))
    twitter_stream.filter(track=COMPANIES, languages=["en"])


if __name__ == "__main__":

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a socket object
        host = "localhost"  # Get local machine name
        port = 10001  # Reserve a port for your service.
        s.bind((host, port))  # Bind to the port

        print("Listening on port: %s" % str(port))
        s.listen(5)  # Now wait for client connection.
        c, addr = s.accept()  # Establish connection with client.
        print("Received request from: " + str(addr))
        print(c)

        try:
            sendData(c)
        except:
            print("Could not send data")

    finally:
        s.close()


