package com.wikibooks.spark.ch7.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

// 7.4.3절
object WatermarkSample {

  def main(args: Array[String]): Unit = {
    
    // step1
    val spark = SparkSession
      .builder()
      .appName("WatermarkSample")
      .master("local[*]")
      .getOrCreate()

    // step2
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // step3(port번호는 ncat 서버와 동일하게 수정해야 함)
    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9000)
      .option("includeTimestamp", true)
      .load()
      .toDF("words", "timestamp")

    // step4
    val words = lines.select(explode(split('words, " ")).as("word"), 'timestamp)

    // 빠른 결과 확인을 위해서 워터마크 및 윈도우 주기를 초단위로 설정하였음 (실제로는 데이터 처리 기준에 맞춰 분 또는 시간 단위로 설정)
    val wordCount = words.withWatermark("timestamp", "5 seconds")
      .groupBy(window('timestamp, "10 seconds", "5 seconds"), 'word).count

    // step5(완료모드 사용시 오래된 집계결과를 지우지 않음)
    val query = wordCount.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", false)
      .start()

    // step6
    query.awaitTermination()
  }  
}