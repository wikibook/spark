package com.wikibooks.spark.ch2.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

object AccumulatorSample {

  def runBuitInAcc(sc: SparkContext) {
    val acc1 = sc.longAccumulator("invalidFormat")
    val acc2 = sc.collectionAccumulator[String]("invalidFormat2")
    val data = List("U1:Addr1", "U2:Addr2", "U3", "U4:Addr4", "U5;Addr5", "U6:Addr6", "U7::Addr7")
    sc.parallelize(data, 3).foreach { v =>
      if (v.split(":").length != 2) {
        acc1.add(1L)
        acc2.add(v)
      }
    }
    println("잘못된 데이터 수:" + acc1.value)
    println("잘못된 데이터:" + acc2.value)
  }

  def runCustomAcc(sc: SparkContext) {
    val acc = new RecordAccumulator
    sc.register(acc, "invalidFormat")
    val data = List("U1:Addr1", "U2:Addr2", "U3", "U4:Addr4", "U5;Addr5", "U6:Addr6", "U7::Addr7")
    sc.parallelize(data, 2).foreach { v =>
      if (v.split(":").length != 2) {
        acc.add(Record(1))
      }
    }
    println("잘못된 데이터 수:" + acc.value)
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("AccumulatorSample")
    val sc = new SparkContext(conf)
    runCustomAcc(sc)
    sc.stop
  }
}

class RecordAccumulator extends AccumulatorV2[Record, Long] {

  private var _record = Record(0)

  def isZero: Boolean = _record.amount == 0 && _record.number == 1

  def copy(): AccumulatorV2[Record, Long] = {
    val newAcc = new RecordAccumulator
    newAcc._record = Record(_record.amount, _record.number)
    newAcc
  }

  def reset(): Unit = {
    _record.amount = 0L
    _record.number = 1L
  }

  def add(other: Record): Unit = {
    _record.add(other)
  }

  def merge(other: AccumulatorV2[Record, Long]): Unit = other match {
    case o: RecordAccumulator => _record.add(o._record);
    case _                    => throw new RuntimeException
  }

  def value: Long = {
    _record.amount
  }
}