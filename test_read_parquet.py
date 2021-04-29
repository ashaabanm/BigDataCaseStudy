from ssl import SSLContext
import configs
from pyspark import SparkConf, SparkContext, SQLContext

if __name__ == "__main__":
    # The main script - create our SparkContext
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)

    newDataDF = sqlContext.read.parquet(configs.PARQUET_PATH)

    newDataDF.select("location").show(truncate=False)

