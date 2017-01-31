package com.wikibooks.spark.ch7.scala

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.OutputMode
import java.io.File

import scala.io.Source

object WordCount {

  // 7.2절
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._

    // step1
    val spark = SparkSession
      .builder()
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // step3(port번호는 ncat 서버와 동일하게 수정해야 함)
    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9000)
      .load()

    // step4
    val words = lines.select(explode(split('value, " ")).as("word"))
    val wordCount = words.groupBy("word").count

    // step5 (아래 foreachQuery 메서드와 같은 형태로도 사용 가능)
    val query = wordCount.writeStream
      .outputMode(OutputMode.Complete)
      .format("console")
      .start()

    // step6
    query.awaitTermination()
  }
}