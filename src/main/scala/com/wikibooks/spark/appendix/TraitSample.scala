package com.wikibooks.spark.appendix

trait A {
  def hello() = println("hello, A")
}

trait B extends A {
  override def hello() = println("hello, B")
}

class C extends A with B

object Test {
  def main(args: Array[String]): Unit = {
    val obj = new C()
    obj.hello()
  }
}
