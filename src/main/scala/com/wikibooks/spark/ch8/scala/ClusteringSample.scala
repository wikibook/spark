package com.wikibooks.spark.ch8.scala

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeansModel

object ClusteringSample {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("ClusteringSample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 원본데이터
    // http://data.seoul.go.kr/openinf/sheetview.jsp?infId=OA-13061&tMenu=11
    // 서울시 공공와이파이 위치정보 (영어)
    val d1 = spark.read.option("header", "true")
      .option("sep", ",")
      .option("inferSchema", true)
      .option("mode", "DROPMALFORMED")
      .csv("/Users/beginspark/Temp/data3.csv")
    d1.printSchema()

    val d2 = d1.toDF("number", "name", "SI", "GOO", "DONG", "x", "y", "b_code", "h_code", "utmk_x", "utmk_y", "wtm_x", "wtm_y")

    val d3 = d2.select('GOO.as("loc"), 'x, 'y)

    d3.show(5, false)

    val indexer = new StringIndexer().setInputCol("loc").setOutputCol("loccode")

    val assembler = new VectorAssembler()
      .setInputCols(Array("loccode", "x", "y"))
      .setOutputCol("features")

    val kmeans = new KMeans().setK(5).setSeed(1L).setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(indexer, assembler, kmeans))

    val model = pipeline.fit(d3)

    val d4 = model.transform(d3)

    d4.groupBy("prediction").agg(collect_set("loc").as("loc"))
      .orderBy("prediction").show(100, false)

    val WSSSE = model.stages(2).asInstanceOf[KMeansModel].computeCost(d4)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    println("Cluster Centers: ")
    model.stages(2).asInstanceOf[KMeansModel].clusterCenters.foreach(println)

  }
}