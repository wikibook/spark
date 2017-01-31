package com.wikibooks.spark.ch10.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph.graphToGraphOps

object PregelSample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("PregelSample")

    val sc = new SparkContext(conf)

    val edges = sc.parallelize(Seq(
      Edge(1L, 2L, 10),
      Edge(1L, 4L, 6),
      Edge(3L, 2L, 7),
      Edge(4L, 3L, -3)))

    val g1 = Graph.fromEdges(edges, 0)
    println("Before: " + g1.triplets.collect.mkString(", "))

    val vprog = (id: VertexId, value: Int, message: Int) => {
      value + message
    }

    val sendMsg = (triplet: EdgeTriplet[Int, Int]) => {
      val diff = triplet.srcAttr - triplet.dstAttr - triplet.attr
      Math.signum(diff) match {
        case 0 => Iterator.empty
        case 1.0f => Iterator((triplet.srcId, -1))
        case -1.0f => Iterator((triplet.srcId, 1))
      }
    }

    def merge(msg1: Int, msg2: Int): Int = {
      msg1 + msg2
    }

    val g2 = g1.pregel(0)(vprog, sendMsg, merge)
    println("After: " + g2.triplets.collect.mkString(", "))
  }
}