package com.wikibooks.spark.ch10.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx._

object GraphOpSample extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("GraphOpSample")

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

  val g1 = Graph(vertices, edges, "X")

  println(g1.numVertices, g1.numEdges)

  println(g1.inDegrees.collect.mkString(", "))
  println(g1.outDegrees.collect.mkString(", "))
  println(g1.degrees.collect.mkString(", "))

  println(g1.vertices.collect.mkString(", "))
  println(g1.edges.collect.mkString(", "))
  println(g1.triplets.collect.mkString(", "))
  println(g1.triplets.filter(_.dstAttr == "P2").collect.mkString(", "))

  val g2 = g1.mapVertices((id, props) => id + "=>" + props)
  val g3 = g1.mapEdges((e) => e.dstId + " to " + e.srcId)
  val g4 = g1.mapTriplets((t) => t.dstId + " to " + t.srcId)

  println(g2.vertices.collect.mkString(", "))
  println(g3.edges.collect.mkString(", "))
  println(g4.triplets.collect().mkString(", "))

  val mapPartition = (id: PartitionID, it: Iterator[Edge[String]]) => {
    it.map(e => e.dstId + " to " + e.srcId)
  }

  val g5 = g1.mapEdges(mapPartition)
  println(g5.edges.collect.mkString(", "))

  println(g1.reverse.edges.collect.mkString(", "))

  val fn1 = (e: EdgeTriplet[String, String]) => e.dstAttr == "P2"
  val fn2 = (id: VertexId, props: String) => props != "P4"

  val g6 = g1.subgraph(epred = fn1)
  val g7 = g1.subgraph(vpred = fn2)

  println(g6.triplets.collect.mkString(", "))
  println(g7.triplets.collect.mkString(", "))

  val g8 = g1.mask(g7)
  println(g8.triplets.collect.mkString(", "))

  val edges2 = sc.parallelize(Seq(
    Edge(1L, 2L, 1),
    Edge(2L, 1L, 1),
    Edge(1L, 2L, 1)))

  val g9 = Graph.fromEdges(edges2, 1)

  val g10 = g9.groupEdges((e1, e2) => e1 + e2)
  println(g10.edges.collect.mkString(", "))

  val g11 = g9.partitionBy(PartitionStrategy.RandomVertexCut).groupEdges((e1, e2) => e1 + e2)
  println(g11.edges.collect.mkString(", "))

  val rdd1 = sc.parallelize(Seq((1L, "AA"), (2L, "BB"), (3L, "CC")))

  val g12 = g1.joinVertices(rdd1)((id, vd, u) => u + "-" + vd)
  val g13 = g1.outerJoinVertices(rdd1)((id, vd, u) => u.getOrElse("XX") + "-" + vd)
  println(g12.vertices.collect.mkString(", "))
  println(g13.vertices.collect.mkString(", "))

  val vrdd1 = g1.collectNeighborIds(EdgeDirection.In)
  val vrdd2 = g1.collectNeighbors(EdgeDirection.In)

  vrdd1.collect.foreach(v => println(v._1 + ":" + v._2.mkString(", ")))
  vrdd2.collect.foreach(v => println(v._1 + ":" + v._2.mkString(", ")))

  val edges3 = sc.parallelize(Seq(
    Edge(1L, 5L, 1),
    Edge(2L, 5L, 1),
    Edge(3L, 5L, 1),
    Edge(4L, 1L, 1),
    Edge(5L, 1L, 1),
    Edge(2L, 3L, 1)
  ))

  val g14 = Graph.fromEdges(edges3, 1)

  val sendMsg = (e: EdgeContext[Int, Int, Int]) => {
    e.sendToDst(e.attr)
  }

  val mergeMsg = (msg1: Int, msg2: Int) => {
    msg1 + msg2
  }

  val vrdd3 = g14.aggregateMessages(sendMsg, mergeMsg)
  vrdd3.collect.foreach(v => println(v._1 + ":" + v._2))
}