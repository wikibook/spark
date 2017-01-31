from pyspark import SparkContext, SparkConf
from pyspark.streaming.context import StreamingContext


def updateFunc(newValues, currentValue):
    if currentValue is None:
        currentValue = 0
    return sum(newValues, currentValue)


def createSSC():
    # ssc 생성
    conf = SparkConf()
    sc = SparkContext(master="local[*]", appName="CheckpointSample", conf=conf)
    ssc = StreamingContext(sc, 3)

    # DStream 생성
    ids1 = ssc.socketTextStream("127.0.0.1", 9000)
    ids2 = ids1.flatMap(lambda v: v.split(" ")).map(lambda v: (v, 1))

    # updateStateByKey
    ids2.updateStateByKey(updateFunc).pprint()

    # checkpoint
    ssc.checkpoint("./checkPoints/checkPointSample/Python")

    # return
    return ssc

ssc = StreamingContext.getOrCreate("./checkPoints/checkPointSample/Python", createSSC)
ssc.start()
ssc.awaitTerminationOrTimeout()