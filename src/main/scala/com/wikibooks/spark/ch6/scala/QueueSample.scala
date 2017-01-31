package com.wikibooks.spark.ch6.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object QueueSample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("QueueSample")

    val ssc = new StreamingContext(conf, Seconds(1))

    val rdd1 = ssc.sparkContext.parallelize(List("a", "b", "c"))
    val rdd2 = ssc.sparkContext.parallelize(List("c", "d", "e"))
    val queue = mutable.Queue(rdd1, rdd2)

    val ds = ssc.queueStream(queue)

    ds.print()

    ssc.start()
    ssc.awaitTermination()
  }
}