package com.wikibooks.spark.ch10.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

object VertextRDDSample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("VertextRDDSample")

    val sc = new SparkContext(conf)
    val vertices = sc.parallelize(List((1L, 10), (2L, 20), (3L, 30)))
    val edges = sc.parallelize(List(Edge(1L, 2L, 1), Edge(2L, 3L, 2), Edge(3L, 1L, 3)))
    val graph = Graph(vertices, edges)

    val vertexRDD = graph.vertices

    val g1 = vertexRDD.mapValues(_ * 10)

    val g2 = vertexRDD.filter(_._2 > 20)

    val otherVertexRDD: RDD[(VertexId, Int)] = sc.parallelize(List((1L, 10), (2L, 700)))

    val g3 = vertexRDD.minus(otherVertexRDD)

    val g4 = vertexRDD.diff(otherVertexRDD)

    val g5 = vertexRDD.leftJoin(otherVertexRDD)((id, lvalue, rvalue) => {
      (lvalue, rvalue.getOrElse(-1))
    })

    val g6 = vertexRDD.innerJoin(otherVertexRDD)((id, lvalue, rvalue) => {
      (lvalue, rvalue)
    })

    val otherVertexRDD2: RDD[(VertexId, Int)] = sc.parallelize(List((1L, 10), (2L, 10), (2L, 10), (5L, 10), (3L, 10)))
    val g7 = vertexRDD.aggregateUsingIndex(otherVertexRDD2, (a: Int, b: Int) => a + b)

    println(vertexRDD.collect.mkString(", "))
    println(g1.collect.mkString(", "))
    println(g2.collect.mkString(", "))
    println(g3.collect.mkString(", "))
    println(g4.collect.mkString(", "))
    println(g5.collect.mkString(", "))
    println(g6.collect.mkString(", "))
    println(g7.collect.mkString(", "))
  }
}