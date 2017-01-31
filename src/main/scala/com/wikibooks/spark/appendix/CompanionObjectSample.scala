package com.wikibooks.spark.appendix

class MyClass private(var name: String) {
  def sayHello(): Unit = {
    MyClass.sayHello()
  }
}

object MyClass {
  def sayHello(): Unit = {
    println("Hello" + new MyClass("sungmin").name)
  }
}
