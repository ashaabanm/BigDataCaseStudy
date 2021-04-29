import json
import tweepy
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
                                            kafkaParams={"metadata.broker.list":configs.BROKER})
sqlContext = SQLContext(sc)


def ananlysis(a):
    return (0)

def retweet(api, msg, tweetId, screen_name):
    api.update_status(msg + " @" + screen_name, tweetId)


def logic(record):
    print(record[1])
    # writeParquet(sqlContext,record[1])


def sendRecord(rdd):
    print(rdd.isEmpty())
    if (rdd.isEmpty() == False):
        # rdd.foreach(lambda record: logic2(record))
        rdd2=rdd.map(lambda record: convertRecToDic(record)).collect()
        writeParquet(sqlContext, rdd2)

def convertRecToDic(record):
    tweet_uni = record[1]  # value
    tweet_str = json.loads(tweet_uni).encode("utf-8")
    tweet_obj = jsonpickle.decode(tweet_str)
    my_dict = HelperFile.Twitter().filterTweet(tweet_obj)
    my_dict = HelperFile.dict_clean(my_dict)
    print(my_dict)
    print(type(my_dict))
    return my_dict


kafkaStream.foreachRDD(sendRecord)

# ssc.checkpoint(".")
ssc.start()
ssc.awaitTermination()
