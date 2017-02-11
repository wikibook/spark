package com.wikibooks.spark.ch3.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

// 3.2.1.5.3절
object WordCount {

  def run(inputPath: String, outputPath: String) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    sc.textFile(inputPath)
      .flatMap(_.split(" "))
      .map((_, 1L))
      .reduceByKey(_ + _)
      .saveAsTextFile(outputPath)

    sc.stop()
  }

  def main(args: Array[String]) {

    val numberOfArgs = 2

    if (args.length == numberOfArgs) {
      run(args(0), args(1))
    } else {
      println("Usage: $SPARK_HOME/bin/spark-submit --class <class_name> --master <master> --<option> <option_value> <jar_file_path> <input_path> <output_path>")
    }
  }
}