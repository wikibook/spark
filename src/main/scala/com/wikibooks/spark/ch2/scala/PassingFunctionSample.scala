package com.wikibooks.spark.ch2.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class PassingFunctionSample {

  var increment = 1

  def add(i: Int): Int = {
    i + 1
  }

  // PassingFunctionSample 전체가 전달되는 잘못된 예
  def runMapSample(sc: SparkContext) {
    val rdd1 = sc.parallelize(1 to 10)
    // java.io.NotSerializableException !!!!
    val rdd2 = rdd1.map(add)
    println(rdd2.collect())
  }

  // object를 이용하여 제대로 구현한 경우 
  def runMapSample2(sc: SparkContext) {
    val rdd1 = sc.parallelize(1 to 10)
    val rdd2 = rdd1.map(Operations.add)
    print(rdd2.collect().toList)
  }

  // 인스턴스 변수를 잘못 전달하는 예 
  def runMapSample3(sc: SparkContext) {
    val rdd1 = sc.parallelize(1 to 10)
    val rdd2 = rdd1.map(_ + increment)
    print(rdd2.collect.toList)
  }

  // 인스턴스 변수를 지역 변수로 바꿔서 전달하는 예  
  def runMapSample4(sc: SparkContext) {
    val rdd1 = sc.parallelize(1 to 10)
    val localIncrement = increment
    val rdd2 = rdd1.map(_ + localIncrement)
    print(rdd2.collect().toList)
  }
}

object Operations {
  def add(i: Int): Int = {
    i + 1;
  }
}

object PassingFunctionSampleRunner {

  def main(args: Array[String]) {
    val sc = getSparkContext
    val sample = new PassingFunctionSample

    // 실행할 메서드 주석 제거 후 실행

    // sample.runMapSample(sc)
    // sample.runMapSample2(sc)
    // sample.runMapSample3(sc)
    // sample.runMapSample4(sc)

    sc.stop
  }

  def getSparkContext(): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("PassingFunctionSample")
    new SparkContext(conf)
  }
}
