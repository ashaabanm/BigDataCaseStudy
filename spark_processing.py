import json
from datetime import datetime
import jsonpickle
from pyspark.sql.context import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import HelperFile
import configs
from parquet_helper import *

twitter_obj = HelperFile.Twitter()
twitter_api = twitter_obj.api_obj()


def spark_context_creator():
    conf = SparkConf()
    conf.setAppName("ConnectingDotsSparkKafkaStreaming")
    sc = None

    try:
        sc.stop()
        sc = SparkContext(conf=conf)
    except:
        sc = SparkContext(conf=conf)
    return sc


sc = spark_context_creator()

# sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 5)

kafkaStream = KafkaUtils.createDirectStream(ssc, topics=[configs.TOPIC_NAME],
                                            kafkaParams={"metadata.broker.list": configs.BROKER})
sqlContext = SQLContext(sc)


def replying_msg(name,polarity):
    msg = ""
    if (polarity < -0.3):
        msg = "Sorry " + name + " there are misunderstanding happened please send us your phone to contact with you"
    elif (polarity > 0.3):
        msg = "thanks " + name + " for using our service"
    else:
        msg = "thanks " + name + " for using our service and if there any proplems plaese contact with customer services"
    return msg + " , time is: " + str(datetime.now())


def retweet(msg, screen_name, tweetId):
    twitter_api.update_status(msg + " @" + screen_name, tweetId)


def send_tweet(record):
    retweet(record["retweet"], record["user_name"], record["tweet_id"])
    print(record)
    print("===================================")


def rdd_processing(rdd):
    print(rdd.isEmpty())
    if (rdd.isEmpty() == False):  # check if rdd batch is empty or not
        print("i rceived")
        rec_list = rdd.map(lambda record: parse_tweet(record)).collect()
        for rec in rec_list:
            send_tweet(rec)
        write_parquet(sqlContext, rec_list)


def parse_tweet(record):
    tweet = record[1]  # take value value
    tweet_str = json.loads(tweet).encode("utf-8")  # convert tweet to string
    tweet_obj = jsonpickle.decode(tweet_str)  # convert tweet string to object
    dict = twitter_obj.convert_tweet_to_dict(tweet_obj)
    cleaned_dict = HelperFile.clean_dic(dict)
    cleaned_dict["retweet"] = replying_msg(dict["name"], dict["polarity"])  # add retweet to dictionary
    return cleaned_dict


kafkaStream.foreachRDD(rdd_processing)

# ssc.checkpoint(".")
ssc.start()
ssc.awaitTermination()
