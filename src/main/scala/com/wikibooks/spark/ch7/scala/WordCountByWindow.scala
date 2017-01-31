package com.wikibooks.spark.ch7.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import java.util.Calendar

// 7.4.2절
object WordCountByWindow {

  def main(args: Array[String]): Unit = {

    // step1
    val spark = SparkSession
      .builder()
      .appName("WordCountByWindow")
      .master("local[*]")
      .getOrCreate()

    // step2
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // step3(port번호는 ncat 서버와 동일하게 수정해야 함)
    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9000)
      .option("includeTimestamp", true)
      .load()
      .toDF("words", "ts")

    // step4
    val words = lines.select(explode(split('words, " ")).as("word"), window('ts, "10 minute", "5 minute").as("time"))
    val wordCount = words.groupBy("time", "word").count

    // step5
    val query = wordCount.writeStream
      .outputMode(OutputMode.Complete)
      .option("truncate", false)
      .format("console")
      .start()

    // step6
    query.awaitTermination()
  }
}