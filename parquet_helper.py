from pyspark import Row
import configs


def writeParquet(sqlContext,data):
    df = sqlContext.createDataFrame([Row(**i) for i in data])
    #df.show()
    df.coalesce(1).write.mode('append').parquet(configs.PARQUET_PATH)


def readParquet(sqlContext):
    newDataDF = sqlContext.read.parquet(configs.PARQUET_PATH)
    # newDataDF = sqlContext.read.parquet(r"hdfs:///tmp/output/people.parquet")
    # newDataDF = sqlContext.read.parquet("hdfs://0.0.0.0:19000/Sales.parquet")
    newDataDF.show()
