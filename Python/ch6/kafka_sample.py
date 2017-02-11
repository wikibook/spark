# 6.2.4절 예제 6-12
from pyspark import SparkContext, SparkConf, storagelevel
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

## pyspark에서 실행할 경우 sparkContext는 생성하지 않습니다!
# ./pyspark --packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.0.2
conf = SparkConf()
sc = SparkContext(master="local[*]", appName="KafkaSample", conf=conf)
ssc = StreamingContext(sc, 3)

ds1 = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group1", {"test": 3})
ds2 = KafkaUtils.createDirectStream(ssc, ["test"], {"metadata.broker.list": "localhost:9092"})

ds1.pprint()
ds2.pprint()

ssc.start()
ssc.awaitTermination()
