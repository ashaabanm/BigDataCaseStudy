import json
import tweepy
from HelperFile import Twitter
from datetime import datetime
import jsonpickle
from pyspark.sql.context import SQLContext
from pyspark import Row, RDD
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import HelperFile
import configs

from parquet_helper import writeParquet


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


def replyingMsg(polarity, name):
    msg = ""
    if (polarity < -0.3):
        msg = "Sorry " + name + " there are misunderstanding happened please send us your phone to contact with you"
    elif (polarity > 0.3):
        msg = "thanks " + name + " for using our service"
    else:
        msg = "thanks " + name + " for using our service and if there any proplems plaese contact with customer services"
    return msg + " " + str(datetime.now())


def retweet(api, tweetId, polarity, name, screen_name):
    api.update_status(replyingMsg(polarity, name) + " @" + screen_name, tweetId)


def logic(record):
    print("===================================")
    twit_api=Twitter().apiObj()
    retweet(twit_api,record["tweet_id"],record["polarity"],record["name"],record["user_name"])
    print(record)


def sendRecord(rdd):
    print(rdd.isEmpty())
    if (rdd.isEmpty() == False):
        rdd2 = rdd.map(lambda record: convertRecToDic(record)).collect()
        for rec in rdd2:
          logic(rec)
        writeParquet(sqlContext, rdd2)


def convertRecToDic(record):
    tweet_uni = record[1]  # value
    tweet_str = json.loads(tweet_uni).encode("utf-8")
    tweet_obj = jsonpickle.decode(tweet_str)
    my_dict = HelperFile.Twitter().filterTweet(tweet_obj)
    my_dict = HelperFile.dict_clean(my_dict)
    return my_dict


kafkaStream.foreachRDD(sendRecord)

# ssc.checkpoint(".")
ssc.start()
ssc.awaitTermination()
