package com.wikibooks.spark.ch6.scala

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Queue

// 6.1.1절 예제 6-1
object StreamingSample {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SteamingSample")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))

    val rdd1 = sc.parallelize(List("Spark Streaming Sample ssc"))
    val rdd2 = sc.parallelize(List("Spark Quque Spark API"))
    val inputQueue = Queue(rdd1, rdd2)
    val lines = ssc.queueStream(inputQueue, true)
    val words = lines.flatMap(_.split(" "))
    words.countByValue().print()

    ssc.start()
    ssc.awaitTermination
  }
}