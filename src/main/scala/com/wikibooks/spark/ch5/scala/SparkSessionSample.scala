package com.wikibooks.spark.ch5.scala

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSessionSample {

  def main(args: Array[String]) {

    // ex5-1, ex5-12
    val spark = SparkSession
      .builder()
      .appName("SparkSessionSample")
      .master("local[*]")
      .getOrCreate()

    // ex5-4
    val source = "file:///Users/beginspark/Apps/spark/README.md"
    val df = spark.read.text(source)

    // ex5-7
    runUntypedTransformationsExample(spark, df)

    // ex5-10
    runTypedTransformationsExample(spark, df)

    spark.stop
  }

  // ex5-7
  def runUntypedTransformationsExample(spark: SparkSession, df: DataFrame): Unit = {

    import org.apache.spark.sql.functions._

    val wordDF = df.select(explode(split(col("value"), " ")).as("word"))
    val result = wordDF.groupBy("word").count

    result.show
  }

  // ex5-10
  def runTypedTransformationsExample(spark: SparkSession, df: DataFrame): Unit = {

    import spark.implicits._

    val ds = df.as[(String)]
    val wordDF = ds.flatMap(_.split(" "))
    val result = wordDF.groupByKey(v => v).count

    result.show
  }
}