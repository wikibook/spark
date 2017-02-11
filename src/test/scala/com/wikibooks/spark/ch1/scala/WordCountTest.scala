package com.wikibooks.spark.ch1.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

import scala.collection.mutable.ListBuffer

// 1.4.1절
class WordCountSpec {
  
  @Test
  def test() {

    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("WordCountTest")

    val sc = new SparkContext(conf)
    val input = new ListBuffer[String]
    input += "Apache Spark is a fast and general engine for large-scale data processing."
    input += "Spark runs on both Windows and UNIX-like systems"
    input.toList

    val inputRDD = sc.parallelize(input)
    val resultRDD = WordCount.process(inputRDD)
    val resultMap = resultRDD.collectAsMap

    assert(resultMap("Spark") == 2)
    assert(resultMap("and") == 2)
    assert(resultMap("runs") == 1)

    println(resultMap)
    
    sc.stop
  }
}