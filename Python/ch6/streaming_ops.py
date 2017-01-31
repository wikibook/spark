from pyspark import SparkContext, SparkConf
from pyspark.streaming.context import StreamingContext

checkpointDir = "./checkPoints/StreamingOps/Scala"
conf = SparkConf()
sc = SparkContext(master="local[*]", appName="StreamingOps", conf=conf)
ssc = StreamingContext(sc, 1)

rdd1 = ssc.sparkContext.parallelize(["a", "b", "c", "c", "c"])
rdd2 = ssc.sparkContext.parallelize(["1,2,3,4,5"])
rdd3 = ssc.sparkContext.parallelize([("k1", "r1"), ("k2", "r2"), ("k3", "r3")])
rdd4 = ssc.sparkContext.parallelize([("k1", "s1"), ("k2", "s2")])
rdd5 = ssc.sparkContext.range(1, 6)

q1 = [rdd1]
q2 = [rdd2]
q3 = [rdd3]
q4 = [rdd4]
q5 = [rdd5]

ds1 = ssc.queueStream(q1, True)
ds2 = ssc.queueStream(q2, True)
ds3 = ssc.queueStream(q3, True)
ds4 = ssc.queueStream(q4, True)
ds5 = ssc.queueStream(q5, True)

# 6.3.1절(파이썬의 경우 print가 아닌 pprint임)
# ds1.pprint()

# 6.3.2절
# ds1.map(lambda v: (v, 1)).pprint()

# 6.3.3절
# ds2.flatMap(lambda v: v.split(",")).pprint()

# 6.3.4절
# ds1.count().pprint()
# ds1.countByValue().pprint()

# 6.3.5절
# ds1.reduce(lambda v1, v2 : v1 + "," + v2).pprint()
# ds1.map(lambda v: (v, 1)).reduceByKey(lambda v1, v2:v1 + v2).pprint()

# 6.3.6절
# ds1.filter(lambda v: v != "c").pprint()

# 6.3.7절
# ds1.union(ds2).pprint()

# 6.3.8절
# ds3.join(ds4).pprint()

# 6.4.1절
# other = ssc.sparkContext.range(1, 3)
# ds5.transform(lambda v: v.subtract(other)).pprint()

# 6.4.2절
t1 = ssc.sparkContext.parallelize(["a", "b", "c"])
t2 = ssc.sparkContext.parallelize(["b", "c"])
t3 = ssc.sparkContext.parallelize(["a", "a", "a"])
q6 = [t1, t2, t3]
ds6 = ssc.queueStream(q6, True)

ssc.checkpoint(checkpointDir)


def updateFunc(newValues, currentValue):
    if currentValue is None:
        currentValue = 0
    return sum(newValues, currentValue)


# ds6.map(lambda v: (v, 1)).updateStateByKey(updateFunc).pprint()

# 6.4.3절
sc = ssc.sparkContext
input = [sc.parallelize([i]) for i in range(1, 100)]
ds7 = ssc.queueStream(input)


# 6.4.4절
# ds7.window(3, 2).pprint()

# 6.4.5절
# ds7.countByWindow(3, 2).pprint()

# 6.4.6절(파이썬의 경우 reduceByWindow에서 역리듀스 함수 사용함)
def invFnc(v1, v2):
    return v1 - v2


def reduceFnc(v1, v2):
    return v1 + v2


# ds7.reduceByWindow(reduceFunc=reduceFnc, invReduceFunc=invFnc, windowDuration=3, slideDuration=2).pprint();
# ds7.map(lambda v: ("sum", v)).reduceByKeyAndWindow(reduceFnc, invFnc, 3, 2).pprint()

# 6.5.1절
ds6.saveAsTextFiles("/Users/beginspark/Temp/test", "dir")

# 6.5.2절
# foreach_sample.py 참고

# 6.6절
# checkpoint_sample.py 참고

ssc.start()
ssc.awaitTermination()
