import collections
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming.context import StreamingContext

Person = collections.namedtuple('Person', 'name age')

conf = SparkConf()
sc = SparkContext(master="local[*]", appName="ForeachSample", conf=conf)
ssc = StreamingContext(sc, 1)

rdd1 = ssc.sparkContext.parallelize([Person("P1", 20)])
rdd2 = ssc.sparkContext.parallelize([Person("P2", 10)])
queue = [rdd1, rdd2]

ds = ssc.queueStream(queue)


def dataFrameOp(rdd):
    spark = SparkSession \
        .builder \
        .appName("sample") \
        .master("local[*]") \
        .getOrCreate()
    if rdd.count() > 0:
        df = spark.createDataFrame(rdd)
        df.select("name", "age").show()

ds.foreachRDD(lambda rdd: dataFrameOp(rdd))

ssc.start()
ssc.awaitTermination()
