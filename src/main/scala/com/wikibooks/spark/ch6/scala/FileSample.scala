package com.wikibooks.spark.ch6.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileSample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("FileSample")

    val ssc = new StreamingContext(conf, Seconds(3))

    val ds = ssc.textFileStream("file:///Users/beginspark/Temp")

    ds.print()

    ssc.start()
    ssc.awaitTermination()
  }
}