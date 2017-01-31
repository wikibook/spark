package com.wikibooks.spark.ch8.scala

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

object VectorSample {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("VectorSample")
      .master("local[*]")
      .getOrCreate()

    val v1 = Vectors.dense(0.1, 0.0, 0.2, 0.3);
    val v2 = Vectors.dense(Array(0.1, 0.0, 0.2, 0.3))
    val v3 = Vectors.sparse(4, Seq((0, 0.1), (2, 0.2), (3, 0.3)))
    val v4 = Vectors.sparse(4, Array(0, 2, 3), Array(0.1, 0.2, 0.3))

    // 8.1.1절
    println(v1.toArray.mkString(", "))
    println(v3.toArray.mkString(", "))

    // 8.1.2절
    val v5 = LabeledPoint(1.0, v1)
    println(s"label:${v5.label}, features:${v5.features}")

    val path = "file:///Users/beginspark/Apps/spark/data/mllib/sample_libsvm_data.txt"
    val v6 = MLUtils.loadLibSVMFile(spark.sparkContext, path)
    val lp1 = v6.first
    println(s"label:${lp1.label}, features:${lp1.features}")

    spark.stop
  }
}