package com.wikibooks.spark.ch2.scala

case class Record(var amount: Long, var number: Long = 1) {
  def map(v: Long) = Record(v)
  def add(amount: Long): Record = {
    add(map(amount))
  }
  def add(other: Record): Record = {
    this.number += other.number
    this.amount += other.amount
    this
  }
  override def toString: String = s"avg:${amount / number}"
}