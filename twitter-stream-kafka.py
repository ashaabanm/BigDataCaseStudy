import json
import jsonpickle
import tweepy
from textblob import TextBlob
from kafka import KafkaProducer
from HelperFile import Twitter
import configs

twitter = Twitter()
class MyStreamListener(tweepy.StreamListener):

    def __init__(self, api):
        self.api = api
        self.me = api.me()
        self.producer = KafkaProducer(bootstrap_servers=configs.BROKER,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def on_status(self, tweet):
        json_tweet = jsonpickle.encode(tweet)
        print(json_tweet)
        self.producer.send(configs.TOPIC_NAME, key=tweet.user.screen_name.encode('ascii', 'ignore'),
                           value=json_tweet)

    def on_error(self, status):
        print("Error detected")
        print(status)

# Create API object
api = twitter.apiObj()

tweets_listener = MyStreamListener(api)
stream = tweepy.Stream(api.auth, tweets_listener)
reviewer = "usa"  # AhmedSh59521052
stream.filter(track=["java"])  # , languages=["en"]

# ========================================================================================== #

# search_query = f"{reviewer} -filter:retweets"
#
# for tweet in Twitter().retrieveTweets(search_query):
#     print(tweet._json['id'])
