package com.wikibooks.spark.ch2.scala

import org.apache.spark.{SparkConf, SparkContext}

object RDDCreateSample {

  def main(arr: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDDCreateSample")

    val sc = new SparkContext(conf)

    // 2.1.3절 예제 2-6
    val rdd1 = sc.parallelize(List("a", "b", "c", "d", "e"))

    // 2.1.3절 예제 2-8
    val rdd2 = sc.textFile("<spark_home_dir>/README.md")

    println(rdd1.collect.mkString(", "))
    println(rdd2.collect.mkString(", "))

    sc.stop
  }
}