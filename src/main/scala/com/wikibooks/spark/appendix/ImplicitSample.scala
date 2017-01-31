package com.wikibooks.spark.appendix

object ImplicitSample {

  def main(args: Array[String]): Unit = {

    case class Person(name: String)

    implicit def stringToPerson(name:String) : Person = Person(name)

    implicit class myClass(name: String) {
      def toPerson: Person = {
        Person(name)
      }
    }

    def sayHello(p: Person): Unit = {
      println("Hello, " + p.name)
    }

    // 문자열 사용
    sayHello("Sungmin!")

    // 문자열의 toPerson 메서드 사용
    sayHello("Sungmin!".toPerson)
  }
}
