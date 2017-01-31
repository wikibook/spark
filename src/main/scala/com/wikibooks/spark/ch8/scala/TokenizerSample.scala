package com.wikibooks.spark.ch8.scala

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.SparkSession

object TokenizerSample {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("TokenizerSample")
      .master("local[*]")
      .getOrCreate()

    val data = Seq("Tokenization is the process", "Refer to the Tokenizer").map(Tuple1(_))
    val inputDF = spark.createDataFrame(data).toDF("input")
    val tokenizer = new Tokenizer().setInputCol("input").setOutputCol("output")
    val outputDF = tokenizer.transform(inputDF)
    outputDF.printSchema()
    outputDF.show(false)

    spark.stop
  }
}