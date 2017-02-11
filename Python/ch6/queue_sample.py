# 6.2.3절

from pyspark import SparkContext, SparkConf
from pyspark.streaming.context import StreamingContext

conf = SparkConf()
sc = SparkContext(master="local[*]", appName="QueueSample", conf=conf)
ssc = StreamingContext(sc, 3)

rdd1 = sc.parallelize(["a", "b", "c"])
rdd2 = sc.parallelize(["c", "d", "e"])

queue = [rdd1, rdd2]

ds = ssc.queueStream(queue)

ds.pprint()

ssc.start()
ssc.awaitTermination()