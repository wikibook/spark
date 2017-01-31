package com.wikibooks.spark.ch10.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}

object EdgeRDDSample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("EdgeRDDSample")

    val sc = new SparkContext(conf)

    val vertices = sc.parallelize(List((1L, 10), (2L, 20), (3L, 30)))
    val edges = sc.parallelize(List(Edge(1L, 2L, 1), Edge(3L, 4L, 2)))
    val graph = Graph(vertices, edges)
    val edgeRDD = graph.edges

    val edge2 = sc.parallelize(List(Edge(1L, 2L, 5), Edge(2L, 1L, 5)))
    val edgeRDD2 = Graph.fromEdges(edge2, 1).edges

    val g1 = edgeRDD.mapValues(_.attr * 10)
    val g2 = edgeRDD.innerJoin(edgeRDD2)((v1, v2, e1, e2) => {
      e1 * e2
    })

    println(g1.collect.mkString(", "))
    println(g2.collect.mkString(", "))
  }
}