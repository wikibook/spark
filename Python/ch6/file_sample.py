from pyspark import SparkContext, SparkConf
from pyspark.streaming.context import StreamingContext

conf = SparkConf()
sc = SparkContext(master="local[*]", appName="FileSample", conf=conf)
ssc = StreamingContext(sc, 3)

ds = ssc.textFileStream("file:///Users/beginspark/Temp")
ds.pprint()

ssc.start()
ssc.awaitTermination()