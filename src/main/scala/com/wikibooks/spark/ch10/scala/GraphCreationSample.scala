package com.wikibooks.spark.ch10.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, PartitionStrategy}

object GraphCreationSample {

  def main(args: Array[String]): Unit = {

    val CURRENT_DIR = this.getClass.getResource("").getPath

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("GraphCreationSample")

    val sc = new SparkContext(conf)

    // apply
    val vertices = sc.parallelize(Seq((1L, "P1"),
      (2L, "P2"),
      (3L, "P3"),
      (4L, "P4"),
      (5L, "P5")))

    val edges = sc.parallelize(Seq(
      Edge(1L, 2L, "1 to 2"),
      Edge(1L, 4L, "1 to 4"),
      Edge(3L, 2L, "3 to 2"),
      Edge(4L, 3L, "4 to 3")))

    val rowEdges = sc.parallelize(Seq(
      (1L, 2L),
      (1L, 4L),
      (3L, 2L),
      (4L, 3L)))

    val g1 = Graph(vertices, edges, "X")
    val g2 = Graph.fromEdges(edges, "X")
    val g3 = Graph.fromEdgeTuples(rowEdges, "X", Some(PartitionStrategy.RandomVertexCut))

    println("g1.triplets:" + g1.triplets.collect.mkString(", "))
    println("g2.triplets:" + g2.triplets.collect.mkString(", "))
    println("g3.triplets:" + g3.triplets.collect.mkString(", "))

    // GraphLoader
    val g4 = GraphLoader.edgeListFile(sc, CURRENT_DIR + "/edgeListFile.txt")
    println("g4.triplets:" + g4.triplets.collect.mkString(", "))
  }
}