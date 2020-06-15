from kafka import KafkaProducer
import kafka
import json
import sys
import tweepy
import os
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# TWITTER API CONFIGURATIONS
consumer_key = os.environ["CONSUMER_KEY"]
consumer_secret = os.environ['CONSUMER_SECRET']
access_token = os.environ['ACCESS_TOKEN']
access_secret = os.environ['ACCESS_SECRET']

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Get Producer that has topic name is Twitter
    # self.producer = self.client.topics[bytes("twitter")].get_producer()

    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter
        self.producer.send("twitter", data.encode('utf-8'))
        #print(data)
        return True

    def on_error(self, status):
        print(status)
        return True

def stream_tweets(hashtag_list):
    twitter_stream = Stream(auth, KafkaPushListener())
    if hashtag_list is not None:
        twitter_stream.filter(track=hashtag_list)

topics = ["coronavirus", "trump", "george floyd", 'biden', "blacklivesmatter"]
stream_tweets(hashtag_list=topics)