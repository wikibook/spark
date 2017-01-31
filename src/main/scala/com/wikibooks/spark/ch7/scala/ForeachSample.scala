package com.wikibooks.spark.ch7.scala

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.streaming.OutputMode

// 7.4.4.4
object ForeachSample {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._

    val spark = SparkSession
      .builder()
      .appName("ForeachSample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // port번호는 ncat 서버와 동일하게 수정해야 함
    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9000)
      .load()

    // step4
    val words = lines.select(explode(split('value, " ")).as("word"))
    val wordCount = words.groupBy("word").count

    // step5(foreach 메서드는 스칼라 및 자바만 사용 가능)
    val query = wordCount
      .writeStream
      .outputMode(OutputMode.Complete)
      .foreach(new ForeachWriter[Row] {
        def open(partitionId: Long, version: Long): Boolean = {
          println(s"partitionId:${partitionId}, version:${version}")
          true
        }

        def process(record: Row) = {
          println("process:" + record.mkString(", "))
        }

        def close(errorOrNull: Throwable): Unit = {
          println("close")
        }
      }).start()

    // step6
    query.awaitTermination()
  }
}