package com.wikibooks.spark.ch6.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 6.2.1절 예제 6-4
object SocketSample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("local[3]")
      .setAppName("SocketSample")

    val ssc = new StreamingContext(conf, Seconds(3))
    val ds = ssc.socketTextStream("localhost", 9000)

    ds.print

    ssc.start()
    ssc.awaitTermination()
  }
}