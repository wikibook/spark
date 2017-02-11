package com.wikibooks.spark.ch2.scala

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object RDDOpSample {

  def main(args: Array[String]) {
    val sc = getSparkContext

    // [예제 실행 방법] 아래에서 원하는 예제의 주석을 제거하고 실행!!

    //    doCollect(sc: SparkContext)
    //    doCount(sc: SparkContext)
    //    doMap(sc: SparkContext)
    //    ex_2_18_flatMap(sc: SparkContext)
    //    doFlatMap(sc: SparkContext)
    //    ex_2_22_flatMap(sc: SparkContext)
    //    doMapPartitions(sc: SparkContext)
    //    doMapPartitionsWithIndex(sc: SparkContext)
    //    doMapValues(sc: SparkContext)
    //    doFlatMapValues(sc: SparkContext)
    //    doZip(sc: SparkContext)
    //    doZipPartitions(sc: SparkContext)
    //    doGroupBy(sc: SparkContext)
    //    doGroupByKey(sc: SparkContext)
    //    doCogroup(sc: SparkContext)
    //    doDistinct(sc: SparkContext)
    //    doCartesian(sc: SparkContext)
    //    doSubtract(sc: SparkContext)
    //    doUnion(sc: SparkContext)
    //    doIntersection(sc: SparkContext)
    //    doJoin(sc: SparkContext)
    //    doLeftOuterJoin(sc: SparkContext)
    //    doSubtractByKey(sc: SparkContext)
    //    doReduceByKey(sc: SparkContext)
    //    doFoldByKey(sc: SparkContext)
    //    doCombineByKey(sc: SparkContext)
    //    doAggregateByKey(sc: SparkContext)
    //    doPipe(sc: SparkContext)
    //    doCoalesceAndRepartition(sc: SparkContext)
    //    doRepartitionAndSortWithinPartitions(sc: SparkContext)
    //    doPartitionBy(sc: SparkContext)
    //    doFilter(sc: SparkContext)
    //    doSortByKey(sc: SparkContext)
    //    doKeysAndValues(sc: SparkContext)
    //    doSample(sc: SparkContext)
    //    doFirst(sc: SparkContext)
    //    doTake(sc: SparkContext)
    //    doTakeSample(sc: SparkContext)
    //    doCountByValue(sc: SparkContext)
    //    doReduce(sc: SparkContext)
    //    doFold(sc: SparkContext)
    //    reduceVsFold(sc: SparkContext)
    //    doAggregate(sc: SparkContext)
    //    doSum(sc: SparkContext)
    //    doForeach(sc: SparkContext)
    //    doForeachPartition(sc: SparkContext)
    //    doToDebugString(sc: SparkContext)
    //    doCache(sc: SparkContext)
    //    doGetPartitions(sc: SparkContext)
    //    saveAndLoadTextFile(sc: SparkContext)
    //    saveAndLoadObjectFile(sc: SparkContext)
    //    saveAndLoadSequenceFile(sc: SparkContext)
    //    testBroadcast(sc: SparkContext)
    sc.stop
  }

  def doCollect(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 10)
    val result = rdd.collect
    println(result.mkString(", "))
  }

  def doCount(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 10)
    val result = rdd.count
    println(result)
  }

  def doMap(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 5)
    val result = rdd.map(_ + 1)
    println(result.collect.mkString(", "))
  }

  def ex_2_18_flatMap(sc: SparkContext) {
    // 단어 3개를 가진 List 생성
    val fruits = List("apple,orange", "grape,apple,mango", "blueberry,tomato,orange")
    // RDD 생성
    val rdd1 = sc.parallelize(fruits)
    // RDD의 map() 메서드로 각 단어를 ","를 기준으로 분리
    val rdd2 = rdd1.map(_.split(","))
    // 결과를 출력
    println(rdd2.collect().map(_.mkString("{", ", ", "}")).mkString("{", ", ", "}"))
  }

  def doFlatMap(sc: SparkContext) {
    val fruits = List("apple,orange", "grape,apple,mango", "blueberry,tomato,orange")
    val rdd1 = sc.parallelize(fruits)
    val rdd2 = rdd1.flatMap(_.split(","))
    print(rdd2.collect.mkString(", "))
  }

  def ex_2_22_flatMap(sc: SparkContext) {
    val fruits = List("apple,orange", "grape,apple,mango", "blueberry,tomato,orange")
    val rdd1 = sc.parallelize(fruits)
    val rdd2 = rdd1.flatMap(log => {
      if (log.contains("apple")) {
        Some(log.indexOf("apple"))
      } else {
        None
      }
    })
    println(rdd2.collect.mkString(","))
  }

  def doMapPartitions(sc: SparkContext) {
    val rdd1 = sc.parallelize(1 to 10, 3)
    val rdd2 = rdd1.mapPartitions(numbers => {
      println("DB연결 !!!")
      numbers.map {
        number => number + 1
      }
    })
    println(rdd2.collect.mkString(", "))
  }

  def doMapPartitionsWithIndex(sc: SparkContext) {
    val rdd1 = sc.parallelize(1 to 10, 3)
    val rdd2 = rdd1.mapPartitionsWithIndex((idx, numbers) => {
      numbers.flatMap {
        case number if idx == 1 => Option(number + 1)
        case _ => None
      }
    })
    println(rdd2.collect.mkString(", "))
  }

  def doMapValues(sc: SparkContext) {
    val rdd = sc.parallelize(List("a", "b", "c")).map((_, 1))
    val result = rdd.mapValues(i => i + 1)
    println(result.collect.mkString("\t"))
  }

  def doFlatMapValues(sc: SparkContext) {
    val rdd = sc.parallelize(Seq((1, "a,b"), (2, "a,c"), (1, "d,e")))
    val result = rdd.flatMapValues(_.split(","))
    println(result.collect.mkString("\t"))
  }

  def doZip(sc: SparkContext) {
    val rdd1 = sc.parallelize(List("a", "b", "c"))
    val rdd2 = sc.parallelize(List(1, 2, 3))
    val result = rdd1.zip(rdd2)
    println(result.collect.mkString(", "))
  }

  def doZipPartitions(sc: SparkContext) {
    val rdd1 = sc.parallelize(List("a", "b", "c"), 3)
    val rdd2 = sc.parallelize(List(1, 2, 3), 3)
    val result = rdd1.zipPartitions(rdd2) {
      (it1, it2) =>
        for {
          v1 <- it1;
          v2 <- it2
        } yield v1 + v2
    }
    println(result.collect.mkString(", "))
  }

  def doGroupBy(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 10)
    val result = rdd.groupBy {
      case i: Int if (i % 2 == 0) => "even"
      case _ => "odd"
    }
    result.collect.foreach {
      v => println(s"${v._1}, [${v._2.mkString(",")}]")
    }
  }

  def doGroupByKey(sc: SparkContext) {
    val rdd = sc.parallelize(List("a", "b", "c", "b", "c")).map((_, 1))
    val result = rdd.groupByKey
    result.collect.foreach {
      v => println(s"${v._1}, [${v._2.mkString(",")}]")
    }
  }

  def doCogroup(sc: SparkContext) {
    val rdd1 = sc.parallelize(List(("k1", "v1"), ("k2", "v2"), ("k1", "v3")))
    val rdd2 = sc.parallelize(List(("k1", "v4")))
    val result = rdd1.cogroup(rdd2)
    result.collect.foreach {
      case (k, (v_1, v_2)) => {
        println(s"($k, [${v_1.mkString(",")}], [${v_2.mkString(",")}])")
      }
    }
  }

  def doDistinct(sc: SparkContext) {
    val rdd = sc.parallelize(List(1, 2, 3, 1, 2, 3, 1, 2, 3))
    val result = rdd.distinct()
    println(result.collect.mkString(", "))
  }

  def doCartesian(sc: SparkContext) {
    val rdd1 = sc.parallelize(List(1, 2, 3))
    val rdd2 = sc.parallelize(List("a", "b", "c"))
    val result = rdd1.cartesian(rdd2)
    println(result.collect.mkString(", "))
  }

  def doSubtract(sc: SparkContext) {
    val rdd1 = sc.parallelize(List("a", "b", "c", "d", "e"))
    val rdd2 = sc.parallelize(List("d", "e"))
    val result = rdd1.subtract(rdd2)
    println(result.collect.mkString(", "))
  }

  def doUnion(sc: SparkContext) {
    val rdd1 = sc.parallelize(List("a", "b", "c"))
    val rdd2 = sc.parallelize(List("d", "e", "f"))
    val result = rdd1.union(rdd2)
    println(result.collect.mkString(", "))
  }

  def doIntersection(sc: SparkContext) {
    val rdd1 = sc.parallelize(List("a", "a", "b", "c"))
    val rdd2 = sc.parallelize(List("a", "a", "c", "c"))
    val result = rdd1.intersection(rdd2)
    println(result.collect.mkString(", "))
  }

  def doJoin(sc: SparkContext) {
    val rdd1 = sc.parallelize(List("a", "b", "c", "d", "e")).map((_, 1))
    val rdd2 = sc.parallelize(List("b", "c")).map((_, 2))
    val result = rdd1.join(rdd2)
    println(result.collect.mkString("\n"))
  }

  def doLeftOuterJoin(sc: SparkContext) {
    val rdd1 = sc.parallelize(List("a", "b", "c")).map((_, 1))
    val rdd2 = sc.parallelize(List("b", "c")).map((_, 2))
    val result1 = rdd1.leftOuterJoin(rdd2)
    val result2 = rdd1.rightOuterJoin(rdd2)
    println("Left: " + result1.collect.mkString("\t"))
    println("Right: " + result2.collect.mkString("\t"))
  }

  def doSubtractByKey(sc: SparkContext) {
    val rdd1 = sc.parallelize(List("a", "b")).map((_, 1))
    val rdd2 = sc.parallelize(List("b")).map((_, 2))
    val result = rdd1.subtractByKey(rdd2)
    println(result.collect.mkString("\n"))
  }

  def doReduceByKey(sc: SparkContext) {
    val rdd = sc.parallelize(List("a", "b", "b")).map((_, 1))
    val result = rdd.reduceByKey(_ + _)
    println(result.collect.mkString(","))
  }

  def doFoldByKey(sc: SparkContext) {
    val rdd = sc.parallelize(List("a", "b", "b")).map((_, 1))
    val result = rdd.foldByKey(0)(_ + _)
    println(result.collect.mkString(","))
  }

  def doCombineByKey(sc: SparkContext) {
    val data = Seq(("Math", 100L), ("Eng", 80L), ("Math", 50L), ("Eng", 70L), ("Eng", 90L))
    val rdd = sc.parallelize(data)
    val createCombiner = (v: Long) => Record(v)
    val mergeValue = (c: Record, v: Long) => c.add(v)
    val mergeCombiners = (c1: Record, c2: Record) => c1.add(c2)
    val result = rdd.combineByKey(createCombiner, mergeValue, mergeCombiners)
    println(result.collect.mkString(",\t"))
  }

  def doAggregateByKey(sc: SparkContext) {
    val data = Seq(("Math", 100L), ("Eng", 80L), ("Math", 50L), ("Eng", 70L), ("Eng", 90L))
    val rdd = sc.parallelize(data)
    val zero = Record(0, 0)
    val mergeValue = (c: Record, v: Long) => c.add(v)
    val mergeCombiners = (c1: Record, c2: Record) => c1.add(c2)
    val result = rdd.aggregateByKey(zero)(mergeValue, mergeCombiners)
    println(result.collect.mkString(",\t"))
  }

  def doPipe(sc: SparkContext) {
    val rdd = sc.parallelize(List("1,2,3", "4,5,6", "7,8,9"))
    val result = rdd.pipe("cut -f 1,3 -d ,")
    println(result.collect.mkString(", "))
  }

  def doCoalesceAndRepartition(sc: SparkContext) {
    val rdd1 = sc.parallelize(1 to 1000000, 10)
    val rdd2 = rdd1.coalesce(5)
    val rdd3 = rdd2.repartition(10);
    println(s"partition size: ${rdd1.getNumPartitions}")
    println(s"partition size: ${rdd2.getNumPartitions}")
    println(s"partition size: ${rdd3.getNumPartitions}")
  }

  def doRepartitionAndSortWithinPartitions(sc: SparkContext) {
    val r = scala.util.Random
    val data = for (i <- 1 to 10) yield (r.nextInt(100), "-")
    val rdd1 = sc.parallelize(data)
    val rdd2 = rdd1.repartitionAndSortWithinPartitions(new HashPartitioner(3))
    rdd2.foreachPartition(it => {
      println("==========");
      it.foreach(v => println(v))
    })
  }

  def doPartitionBy(sc: SparkContext) {
    val rdd1 = sc.parallelize(List("apple", "mouse", "monitor"), 5).map { a => (a, a.length) }
    val rdd2 = rdd1.partitionBy(new HashPartitioner(3))
    println(s"rdd1:${rdd1.getNumPartitions}, rdd2:${rdd2.getNumPartitions}")
  }

  def doFilter(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 5)
    val result = rdd.filter(_ > 2)
    println(result.collect.mkString(", "))
  }

  def doSortByKey(sc: SparkContext) {
    val rdd = sc.parallelize(List("q", "z", "a"))
    val result = rdd.map((_, 1)).sortByKey()
    println(result.collect.mkString(", "))
  }

  def doKeysAndValues(sc: SparkContext) {
    val rdd = sc.parallelize(List(("k1", "v1"), ("k2", "v2"), ("k3", "v3")))
    println(rdd.keys.collect.mkString(","))
    println(rdd.values.collect.mkString(","))
  }

  def doSample(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 100)
    val result1 = rdd.sample(false, 0.5)
    val result2 = rdd.sample(true, 1.5)
    println(result1.take(5).mkString(","))
    println(result2.take(5).mkString(","))
  }

  def doFirst(sc: SparkContext) {
    val rdd = sc.parallelize(List(5, 4, 1))
    val result = rdd.first
    println(result)
  }

  def doTake(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 20, 5)
    val result = rdd.take(5)
    println(result.mkString(", "))
  }

  def doTakeSample(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 100)
    val result = rdd.takeSample(false, 20)
    println(result.length)
  }

  def doCountByValue(sc: SparkContext) {
    val rdd = sc.parallelize(List(1, 1, 2, 3, 3))
    val result = rdd.countByValue
    println(result)
  }

  def doReduce(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 10, 3)
    val result = rdd.reduce(_ + _)
    println(result)
  }

  def doFold(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 10, 3)
    val result = rdd.fold(0)(_ + _)
    println(result)
  }

  def reduceVsFold(sc: SparkContext) {
    // Prod 클래스 선언   
    case class Prod(var price: Int) {
      var cnt = 1
    }
    val rdd = sc.parallelize(List(Prod(300), Prod(200), Prod(100)), 10)
    // reduce 
    val r1 = rdd.reduce((p1, p2) => {
      p1.price += p2.price
      p1.cnt += 1
      p1
    })
    println(s"Reduce: (${r1.price}, ${r1.cnt})")
    // fold
    val r2 = rdd.fold(Prod(0))((p1, p2) => {
      p1.price += p2.price
      p1.cnt += 1
      p1
    })
    println(s"Fold: (${r2.price}, ${r2.cnt})")
  }

  def doAggregate(sc: SparkContext) {
    val rdd = sc.parallelize(List(100, 80, 75, 90, 95), 3)
    val zeroValue = Record(0, 0)
    val seqOp = (r: Record, v: Int) => r.add(v)
    val combOp = (r1: Record, r2: Record) => r1 add r2
    val result1 = rdd.aggregate(zeroValue)(seqOp, combOp)
    println(result1.amount / result1.number)

    // 좀더 간결한 코드 
    val result2 = rdd.aggregate(Record(0, 0))(_ add _, _ add _)
    println(result1.amount / result1.number)
  }

  def doSum(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 10)
    val result = rdd.sum
    println(result)
  }

  def doForeach(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 10, 3)
    rdd.foreach { v =>
      println(s"Value Side Effect: ${v}")
    }
  }

  def doForeachPartition(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 10, 3)
    rdd.foreachPartition(values => {
      println("Partition Side Effect!!")
      for (v <- values) println(s"Value Side Effect: ${v}")
    })
  }

  def doToDebugString(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 100, 10)
      .map(_ * 2).persist
      .map(_ + 1).coalesce(2)
    println(rdd.toDebugString)
  }

  def doCache(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 100, 10)
    val rdd1 = rdd.cache
    val rdd2 = rdd.persist(StorageLevel.MEMORY_ONLY)
  }

  def doGetPartitions(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 1000, 10)
    println(rdd.partitions.size)
    println(rdd.getNumPartitions)
  }

  def saveAndLoadTextFile(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 1000, 3)
    val codec = classOf[org.apache.hadoop.io.compress.GzipCodec]
    // save
    rdd.saveAsTextFile("<path_to_save>/sub1")
    // save(gzip)
    rdd.saveAsTextFile("<path_to_save>/sub2", codec)
    // load
    val rdd2 = sc.textFile("<path_to_save>/sub1")
    println(rdd2.take(10).toList)
  }

  def saveAndLoadObjectFile(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 1000)
    // save
    rdd.saveAsObjectFile("<path_to_save>/sub_path")
    // load!
    val rdd2 = sc.objectFile[Int]("<path_to_save>/sub_path")
    println(rdd2.take(10).mkString(", "))
  }

  def saveAndLoadSequenceFile(sc: SparkContext) {
    val rdd = sc.parallelize(List("a", "b", "c", "b", "c")).map((_, 1))

    // save
    // 아래 경로는 실제 저장 경로로 변경하여 테스트
    rdd.saveAsSequenceFile("data/sample/saveAsSeqFile/scala")

    // load!
    // 아래 경로는 실제 저장 경로로 변경하여 테스트
    val rdd2 = sc.sequenceFile[String, Int]("data/sample/saveAsSeqFile/scala")
    println(rdd2.collect.mkString(", "))
  }

  def testBroadcast(sc: SparkContext) {
    val broadcastUsers = sc.broadcast(Set("u1", "u2"))
    val rdd = sc.parallelize(List("u1", "u3", "u3", "u4", "u5", "u6"), 3)
    val result = rdd.filter(broadcastUsers.value.contains(_))

    println(result.collect.mkString(","))
  }

  def getSparkContext(): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("RDDOpSample")
    new SparkContext(conf)
  }
}