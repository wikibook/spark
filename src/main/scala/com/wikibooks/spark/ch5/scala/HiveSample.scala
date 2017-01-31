package com.wikibooks.spark.ch5.scala

import org.apache.spark.sql.{SaveMode, SparkSession}

// 5.6절 하이브 연동
object HiveSample {

  def main(args: Array[String]) = {

    val spark = SparkSession.builder()
      .appName("Spark Hive Example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/Users/beginspark/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val row1 = Person("hayoon", 7, "student")
    val row2 = Person("sunwoo", 13, "student")
    val row3 = Person("hajoo", 5, "kindergartener")
    val row4 = Person("jinwoo", 13, "student")
    val data = List(row1, row2, row3, row4)
    val ds = spark.createDataset(data)

    sql("CREATE TABLE IF NOT EXISTS Persons (name STRING, age INT, job STRING)")
    spark.sql("show tables").show

    ds.toDF().write.mode(SaveMode.Overwrite).saveAsTable("Users")
    spark.sql("show tables").show

    spark.stop
  }
}