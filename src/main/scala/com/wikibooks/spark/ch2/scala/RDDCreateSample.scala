package com.wikibooks.spark.ch2.scala

import org.apache.spark.{SparkConf, SparkContext}

object RDDCreateSample {

  def main(arr: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDDCreateSample")

    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List("a", "b", "c", "d", "e"))

    // <project_home>은 예제 프로젝트의 경로
    val rdd2 = sc.textFile("file://<project_home>/source/data/sample.txt")

    println(rdd1.collect.mkString(", "))
    println(rdd2.collect.mkString(", "))

    sc.stop
  }
}