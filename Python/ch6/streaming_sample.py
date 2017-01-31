from pyspark import SparkContext, SparkConf
from pyspark.streaming.context import StreamingContext

conf = SparkConf()
sc = SparkContext(master="local[*]", appName="SteamingSample", conf=conf)
ssc = StreamingContext(sc, 3)

rdd1 = sc.parallelize(["Spark Streaming Sample ssc"])
rdd2 = sc.parallelize(["Spark Quque Spark API"])

inputQueue = [rdd1, rdd2]

lines = ssc.queueStream(inputQueue, True)
words = lines.flatMap(lambda v: v.split(" "))
words.countByValue().pprint()

ssc.start()
ssc.awaitTermination()