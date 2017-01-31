package com.wikibooks.spark.ch7.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

// 7.3절
object StructuredStreamSample {

  def runReadStreamSample(spark: SparkSession) {


    // 예제를 실행 후
    // "<spark_home>/examples/src/main/resources/peple.json" 파일을
    // json("..") 메서드에 지정한 디렉토리에 복사하여 결과 확인
    val source = spark
      .readStream
      .schema(new StructType().add("name", "string").add("age", "integer"))
      .json("/Users/beginspark/Temp/json")

    val result = source.groupBy("name").count

    result
      .writeStream
      .outputMode(OutputMode.Complete)
      .format("console")
      .start
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("StructuredStreamSample")
      .master("local[*]")
      .getOrCreate()

    runReadStreamSample(spark)
  }
}