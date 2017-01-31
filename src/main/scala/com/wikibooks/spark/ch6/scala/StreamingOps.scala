package com.wikibooks.spark.ch6.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object StreamingOps {

  def main(args: Array[String]): Unit = {

    val checkpointDir = "./checkPoints/StreamingOps/Scala"
    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingOps")
    val ssc = new StreamingContext(conf, Seconds(1))

    val rdd1 = ssc.sparkContext.parallelize(List("a", "b", "c", "c", "c"))
    val rdd2 = ssc.sparkContext.parallelize(List("1,2,3,4,5"))
    val rdd3 = ssc.sparkContext.parallelize(List(("k1", "r1"), ("k2", "r2"), ("k3", "r3")))
    val rdd4 = ssc.sparkContext.parallelize(List(("k1", "s1"), ("k2", "s2")))
    val rdd5 = ssc.sparkContext.range(1, 6)

    val q1 = mutable.Queue(rdd1)
    val q2 = mutable.Queue(rdd2)
    val q3 = mutable.Queue(rdd3)
    val q4 = mutable.Queue(rdd4)
    val q5 = mutable.Queue(rdd5)

    val ds1 = ssc.queueStream(q1, true)
    val ds2 = ssc.queueStream(q2, true)
    val ds3 = ssc.queueStream(q3, true)
    val ds4 = ssc.queueStream(q4, true)
    val ds5 = ssc.queueStream(q5, true)

    // 6.3.1절
    //ds1.print

    // 6.3.2절
    //ds1.map((_, 1L)).print

    // 6.3.3절
    //ds2.flatMap(_.split(",")).print

    // 6.3.4절
    //ds1.count.print
    //ds1.countByValue().print

    // 6.3.5절
    //ds1.reduce(_ + "," + _).print
    //ds1.map((_, 1L)).reduceByKey(_ + _).print

    // 6.3.6절
    //ds1.filter(_ != "c").print

    // 6.3.7절
    //ds1.union(ds2).print

    // 6.3.8절
    //ds3.join(ds4).print

    // 6.4.1절
    val other = ssc.sparkContext.range(1, 3)
    //ds5.transform(_ subtract other).print

    // 6.4.2절
    val t1 = ssc.sparkContext.parallelize(List("a", "b", "c"))
    val t2 = ssc.sparkContext.parallelize(List("b", "c"))
    val t3 = ssc.sparkContext.parallelize(List("a", "a", "a"))
    val q6 = mutable.Queue(t1, t2, t3)
    val ds6 = ssc.queueStream(q6, true)

    ssc.checkpoint(checkpointDir)

    val updateFunc = (newValues: Seq[Long], currentValue: Option[Long]) => Option(currentValue.getOrElse(0L) + newValues.sum)

    //ds6.map((_, 1L)).updateStateByKey(updateFunc).print

    // 6.4.3절
    val sc = ssc.sparkContext
    val input = for (i <- mutable.Queue(1 to 100: _*)) yield sc.parallelize(i :: Nil)
    val ds7 = ssc.queueStream(input)

    // 6.4.4절
    //ds7.window(Seconds(3), Seconds(2)).print

    // 6.4.5절
    //ds7.countByWindow(Seconds(3), Seconds(2)).print

    // 6.4.6절
    //ds7.reduceByWindow((a, b) => Math.max(a, b), Seconds(3), Seconds(2)).print

    // 6.4.7절
    //ds7.map(v => (v % 2, 1)).reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(4), Seconds(2)).print

    // 역리듀스 함수 사용
    val invFnc = (v1: Int, v2: Int) => {
      v1 - v2
    }

    val reduceFnc = (v1: Int, v2: Int) => {
      v1 + v2
    }

    //ds7.map(v => ("sum", v)).reduceByKeyAndWindow(reduceFnc, invFnc, Seconds(3), Seconds(2)).print

    // 6.4.8절
    //ds7.countByValueAndWindow(Seconds(3), Seconds(2)).print

    // 6.5.1절
    //ds6.saveAsTextFiles("/Users/beginspark/Temp/test", "dir")

    // 6.5.2절
    // ForeachSample 참고

    // 6.6절
    // CheckPointSample 참고

    ssc.start()
    ssc.awaitTerminationOrTimeout(5000)
  }
}

// 6.5.2절
object ForeachSample {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("ForeachSample")
    val ssc = new StreamingContext(conf, Seconds(1))
    val sc = ssc.sparkContext

    val rdd1 = sc.parallelize(Person("P1", 20) :: Nil)
    val rdd2 = sc.parallelize(Person("P2", 10) :: Nil)
    val queue = mutable.Queue(rdd1, rdd2)
    val ds = ssc.queueStream(queue)

    ds.foreachRDD(rdd => {
      val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
      import spark.implicits._
      val df = spark.createDataFrame(rdd)
      df.select("name", "age").show
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(1000 * 5)
  }
}

// 6.6절
object CheckPointSample {

  def updateFnc(newValues: Seq[Int], currentValue: Option[Int]): Option[Int] = {
    Option(currentValue.getOrElse(0) + newValues.sum)
  }

  def createSSC(checkpointDir: String) = {
    // ssc 생성
    val conf = new SparkConf().setMaster("local[*]").setAppName("CheckPointSample")
    val sc = new SparkContext(conf);
    val ssc = new StreamingContext(sc, Milliseconds(3000))
    // DStream 생성
    val ids1 = ssc.socketTextStream("127.0.0.1", 9000)
    val ids2 = ids1.flatMap(_.split(" ")).map((_, 1))
    // updateStateByKey
    ids2.updateStateByKey(updateFnc).print
    // checkpoint
    ssc.checkpoint(checkpointDir)
    // return
    ssc
  }

  def main(args: Array[String]) {
    val checkpointDir = "./checkPoints/checkPointSample/Scala"
    val ssc = StreamingContext.getOrCreate(checkpointDir, () => createSSC(checkpointDir))
    ssc.start()
    ssc.awaitTermination()
  }
}