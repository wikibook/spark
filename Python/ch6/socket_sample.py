from pyspark import SparkContext, SparkConf
from pyspark.streaming.context import StreamingContext

conf = SparkConf()
sc = SparkContext(master="local[*]", appName="SocketSample", conf=conf)
ssc = StreamingContext(sc, 3)

ds = ssc.socketTextStream("localhost", 9000)
ds.pprint()

ssc.start()
ssc.awaitTermination()