import random

from pyspark import SparkContext, SparkConf
from pyspark.storagelevel import StorageLevel

from record import Record


# MapPartions
def increase(numbers):
    print("DB 연결 !!!")
    return (i + 1 for i in numbers)


# MapPartitionsWithIndex
def increaseWithIndex(idx, numbers):
    for i in numbers:
        if (idx == 1):
            yield i


# combineBy
def createCombiner(v):
    return Record(v)


# combineBy
def mergeValue(c, v):
    return c.addAmt(v)


# combineBy
def mergeCombiners(c1, c2):
    return c1 + c2


# Aggregate
def seqOp(r, v):
    return r.addAmt(v)


# Aggregate
def combOp(r1, r2):
    return r1 + r2


# foreachPartition
def sideEffect(values):
    print("Partition Side Effect!!")
    for v in values:
        print("Value Side Effect: %s" % v)


class RDDOpSample():
    def doCollect(self, sc):
        rdd = sc.parallelize(range(1, 11))
        result = rdd.collect()
        print(result)

    def doCount(self, sc):
        rdd = sc.parallelize(range(1, 11))
        result = rdd.count()
        print(result)

    def doMap(self, sc):
        rdd1 = sc.parallelize(range(1, 6))
        rdd2 = rdd1.map(lambda v: v + 1)
        print(rdd2.collect())

    def doFlatMap(self, sc):
        rdd1 = sc.parallelize(["apple,orange", "grape,apple,mango", "blueberry,tomato,orange"])
        rdd2 = rdd1.flatMap(lambda s: s.split(","))
        print(rdd2.collect())

    def doMapPartitions(self, sc):
        rdd1 = sc.parallelize(range(1, 11), 3)
        rdd2 = rdd1.mapPartitions(increase)
        print(rdd2.collect())

    def doMapPartitionsWithIndex(self, sc):
        rdd1 = sc.parallelize(range(1, 11), 3)
        rdd2 = rdd1.mapPartitionsWithIndex(increaseWithIndex)
        print(rdd2.collect())

    def doMapValues(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c"])
        rdd2 = rdd1.map(lambda v: (v, 1))
        rdd3 = rdd2.mapValues(lambda i: i + 1)
        print(rdd3.collect())

    def doFlatMapValues(self, sc):
        rdd1 = sc.parallelize([(1, "a,b"), (2, "a,c"), (1, "d,e")])
        rdd2 = rdd1.flatMapValues(lambda s: s.split(","))
        print(rdd2.collect())

    def doZip(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c"])
        rdd2 = sc.parallelize([1, 2, 3])
        result = rdd1.zip(rdd2)
        print(result.collect())

    def doGroupBy(self, sc):
        rdd1 = sc.parallelize(range(1, 11))
        rdd2 = rdd1.groupBy(lambda v: "even" if v % 2 == 0 else "odd")
        for x in rdd2.collect():
            print(x[0], list(x[1]))

    def doGroupByKey(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c", "b", "c"]).map(lambda v: (v, 1))
        rdd2 = rdd1.groupByKey()
        for x in rdd2.collect():
            print(x[0], list(x[1]))

    def doCogroup(self, sc):
        rdd1 = sc.parallelize([("k1", "v1"), ("k2", "v2"), ("k1", "v3")])
        rdd2 = sc.parallelize([("k1", "v4")])
        result = rdd1.cogroup(rdd2)
        for x in result.collect():
            print(x[0], list(x[1][0]), list(x[1][1]))

    def doDistinct(self, sc):
        rdd = sc.parallelize([1, 2, 3, 1, 2, 3, 1, 2, 3])
        result = rdd.distinct()
        print(result.collect())

    def doCartesian(self, sc):
        rdd1 = sc.parallelize([1, 2, 3])
        rdd2 = sc.parallelize(["a", "b", "c"])
        result = rdd1.cartesian(rdd2)
        print(result.collect())

    def doSubtract(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c", "d", "e"])
        rdd2 = sc.parallelize(["d", "e"])
        result = rdd1.subtract(rdd2)
        print(result.collect())

    def doUnion(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c"])
        rdd2 = sc.parallelize(["d", "e", "f"])
        result = rdd1.union(rdd2)
        print(result.collect())

    def doIntersection(self, sc):
        rdd1 = sc.parallelize(["a", "a", "b", "c"])
        rdd2 = sc.parallelize(["a", "a", "c", "c"])
        result = rdd1.intersection(rdd2)
        print(result.collect())

    def doJoin(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c", "d", "e"]).map(lambda v: (v, 1))
        rdd2 = sc.parallelize(["b", "c"]).map(lambda v: (v, 2))
        result = rdd1.join(rdd2)
        print(result.collect())

    def doLeftOuterJoin(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c"]).map(lambda v: (v, 1))
        rdd2 = sc.parallelize(["b", "c"]).map(lambda v: (v, 2))
        result1 = rdd1.leftOuterJoin(rdd2)
        result2 = rdd1.rightOuterJoin(rdd2)
        print("Left: %s" % result1.collect())
        print("Right: %s" % result1.collect())

    def doSubtractByKey(self, sc):
        rdd1 = sc.parallelize(["a", "b"]).map(lambda v: (v, 1))
        rdd2 = sc.parallelize(["b"]).map(lambda v: (v, 1))
        result = rdd1.subtractByKey(rdd2)
        print(result.collect())

    def doReduceByKey(self, sc):
        rdd = sc.parallelize(["a", "b", "b"]).map(lambda v: (v, 1))
        result = rdd.reduceByKey(lambda v1, v2: v1 + v2)
        print(result.collect())

    def doFoldByKey(self, sc):
        rdd = sc.parallelize(["a", "b", "b"]).map(lambda v: (v, 1))
        result = rdd.foldByKey(0, lambda v1, v2: v1 + v2)
        print(result.collect())

    def doCombineByKey(self, sc):
        rdd = sc.parallelize([("Math", 100), ("Eng", 80), ("Math", 50), ("Eng", 70), ("Eng", 90)])
        result = rdd.combineByKey(lambda v: createCombiner(v), lambda c, v: mergeValue(c, v),
                                  lambda c1, c2: mergeCombiners(c1, c2))
        print('Math', result.collectAsMap()['Math'], 'Eng', result.collectAsMap()['Eng'])

    def doAggregateByKey(self, sc):
        rdd = sc.parallelize([("Math", 100), ("Eng", 80), ("Math", 50), ("Eng", 70), ("Eng", 90)])
        result = rdd.aggregateByKey(Record(0, 0), lambda c, v: mergeValue(c, v), lambda c1, c2: mergeCombiners(c1, c2))
        print('Math', result.collectAsMap()['Math'], 'Eng', result.collectAsMap()['Eng'])

    def doPipe(self, sc):
        rdd = sc.parallelize(["1,2,3", "4,5,6", "7,8,9"])
        result = rdd.pipe("cut -f 1,3 -d ,")
        print(result.collect())

    def doCoalesceAndRepartition(self, sc):
        rdd1 = sc.parallelize(list(range(1, 11)), 10)
        rdd2 = rdd1.coalesce(5)
        rdd3 = rdd2.repartition(10)
        print("partition size: %d" % rdd1.getNumPartitions())
        print("partition size: %d" % rdd2.getNumPartitions())
        print("partition size: %d" % rdd3.getNumPartitions())

    def doRepartitionAndSortWithinPartitions(self, sc):
        data = [random.randrange(1, 100) for i in range(0, 10)]
        rdd1 = sc.parallelize(data).map(lambda v: (v, "-"))
        rdd2 = rdd1.repartitionAndSortWithinPartitions(3, lambda x: x)
        rdd2.foreachPartition(lambda values: print(list(values)))

    def doPartitionBy(self, sc):
        rdd1 = sc.parallelize([("apple", 1), ("mouse", 1), ("monitor", 1)], 5)
        rdd2 = rdd1.partitionBy(3)
        print("rdd1: %d, rdd2: %d" % (rdd1.getNumPartitions(), rdd2.getNumPartitions()))

    def doFilter(self, sc):
        rdd1 = sc.parallelize(range(1, 6))
        rdd2 = rdd1.filter(lambda i: i > 2)
        print(rdd2.collect())

    def doSortByKey(self, sc):
        rdd = sc.parallelize([("q", 1), ("z", 1), ("a", 1)])
        result = rdd.sortByKey()
        print(result.collect())

    def doKeysAndValues(self, sc):
        rdd = sc.parallelize([("k1", "v1"), ("k2", "v2"), ("k3", "v3")])
        print(rdd.keys().collect())
        print(rdd.values().collect())

    def doSample(self, sc):
        rdd = sc.parallelize(range(1, 101))
        result1 = rdd.sample(False, 0.5, 100)
        result2 = rdd.sample(True, 1.5, 100)
        print(result1.take(5))
        print(result2.take(5))

    def doFirst(self, sc):
        rdd = sc.parallelize([5, 4, 1])
        result = rdd.first()
        print(result)

    def doTake(self, sc):
        rdd = sc.parallelize(range(1, 100))
        result = rdd.take(5)
        print(result)

    def doTakeSample(self, sc):
        rdd = sc.parallelize(range(1, 100))
        result = rdd.takeSample(False, 20)
        print(len(result))

    def doCountByValue(self, sc):
        rdd = sc.parallelize([1, 1, 2, 3, 3])
        result = rdd.countByValue()
        for k, v in result.items():
            print(k, "->", v)

    def doReduce(self, sc):
        rdd = sc.parallelize(range(1, 11), 3)
        result = rdd.reduce(lambda v1, v2: v1 + v2)
        print(result)

    def doFold(self, sc):
        rdd = sc.parallelize(range(1, 11), 3)
        result = rdd.fold(0, lambda v1, v2: v1 + v2)
        print(result)

    def doAggregate(self, sc):
        rdd = sc.parallelize([100, 80, 75, 90, 95])
        result = rdd.aggregate(Record(0, 0), seqOp, combOp)
        print(result)

    def doSum(self, sc):
        rdd = sc.parallelize(range(1, 11))
        result = rdd.sum()
        print(result)

    def doForeach(self, sc):
        rdd = sc.parallelize(range(1, 11))
        result = rdd.foreach(lambda v: print("Value Side Effect: %s" % v))

    def doForeachPartition(self, sc):
        rdd = sc.parallelize(range(1, 11), 3)
        result = rdd.foreachPartition(sideEffect)

    def doDebugString(self, sc):
        rdd1 = sc.parallelize(range(1, 100), 10)
        rdd2 = rdd1.map(lambda v: v * 2)
        rdd3 = rdd2.map(lambda v: v + 1)
        rdd4 = rdd3.coalesce(2)
        print(rdd4.toDebugString())

    def doCache(self, sc):
        rdd = sc.parallelize(range(1, 100), 10)
        rdd.cache()
        rdd.persist(StorageLevel.MEMORY_ONLY)
        print(rdd.persist().is_cached)

    def doGetPartitions(self, sc):
        rdd = sc.parallelize(range(1, 100), 10)
        print(rdd.getNumPartitions())

    def saveAndLoadTextFile(self, sc):
        rdd = sc.parallelize(range(1, 1000), 3)
        codec = "org.apache.hadoop.io.compress.GzipCodec"
        # save
        rdd.saveAsTextFile("<path_to_save>/sub1")
        # save(gzip)
        rdd.saveAsTextFile("<path_to_save>/sub2", codec)
        # load
        rdd2 = sc.textFile("<path_to_save>/sub1")
        print(rdd2.take(10))

    def saveAndLoadObjectFile(self, sc):
        rdd = sc.parallelize(range(1, 1000), 3)
        # save
        # 아래 경로는 실제 저장 경로로 변경하여 테스트
        rdd.saveAsPickleFile("data/sample/saveAsObjectFile/python")
        # load
        # 아래 경로는 실제 저장 경로로 변경하여 테스트
        rdd2 = sc.pickleFile("data/sample/saveAsObjectFile/python")
        print(rdd2.take(10))

    def saveAndLoadSequenceFile(self, sc):
        # 아래 경로는 실제 저장 경로로 변경하여 테스트
        path = "data/sample/saveAsSeqFile/python"

        outputFormatClass = "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"
        inputFormatClass = "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"
        keyClass = "org.apache.hadoop.io.Text"
        valueClass = "org.apache.hadoop.io.IntWritable"
        conf = "org.apache.hadoop.conf.Configuration"
        rdd1 = sc.parallelize(["a", "b", "c", "b", "c"])
        rdd2 = rdd1.map(lambda x: (x, 1))
        # save
        rdd2.saveAsNewAPIHadoopFile(path, outputFormatClass, keyClass, valueClass)
        # load
        rdd3 = sc.newAPIHadoopFile(path, inputFormatClass, keyClass, valueClass)
        for k, v in rdd3.collect():
            print(k, v)

    def testBroadcaset(self, sc):
        bu = sc.broadcast(set(["u1", "u2"]))
        rdd = sc.parallelize(["u1", "u3", "u3", "u4", "u5", "u6"], 3)
        result = rdd.filter(lambda v: v in bu.value)
        print(result.collect())


if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.driver.host", "127.0.0.1")
    sc = SparkContext(master="local[*]", appName="RDDOpSample", conf=conf)

    obj = RDDOpSample()

    # [예제 실행 방법] 아래에서 원하는 예제의 주석을 제거하고 실행!!
    # ex) obj.testBroadcaset(sc)
    # obj.doCollect(sc)
    # obj.obj.doCount(sc)
    # obj.doMap(sc)
    # obj.doFlatMap(sc)
    # obj.doMapPartitions(sc)
    # obj.doMapPartitionsWithIndex(sc)
    # obj.doMapValues(sc)
    # obj.doFlatMapValues(sc)
    # obj.doZip(sc)
    # obj.doGroupBy(sc)
    # obj.doGroupByKey(sc)
    # obj.doCogroup(sc)
    # obj.doDistinct(sc)
    # obj.doCartesian(sc)
    # obj.doSubtract(sc)
    # obj.doUnion(sc)
    # obj.doIntersection(sc)
    # obj.doJoin(sc)
    # obj.doLeftOuterJoin(sc)
    # obj.doSubtractByKey(sc)
    # obj.doReduceByKey(sc)
    # obj.doFoldByKey(sc)
    # obj.doCombineByKey(sc)
    # obj.doAggregateByKey(sc)
    # obj.doPipe(sc)
    # obj.doCoalesceAndRepartition(sc)
    # obj.doRepartitionAndSortWithinPartitions(sc)
    # obj.doPartitionBy(sc)
    # obj.doFilter(sc)
    # obj.doSortByKey(sc)
    # obj.doKeysAndValues(sc)
    # obj.doSample(sc)
    # obj.doFirst(sc)
    # obj.doTake(sc)
    # obj.doTakeSample(sc)
    # obj.doCountByValue(sc)
    # obj.doReduce(sc)
    # obj.doFold(sc)
    # obj.doAggregate(sc)
    # obj.doSum(sc)
    # obj.doForeach(sc)
    # obj.doForeachPartition(sc)
    # obj.doDebugString(sc)
    # obj.doCache(sc)
    # obj.doGetPartitions(sc)
    # obj.saveAndLoadTextFile(sc)
    # obj.saveAndLoadObjectFile(sc)
    # obj.saveAndLoadSequenceFile(sc)
    # obj.testBroadcaset(sc)

sc.stop()
