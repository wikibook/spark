package com.wikibooks.spark.ch8.scala

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object ClassficationSample {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("ClassficationSample")
      .master("local[*]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // Label(혼잡:1.0, 원활:0.0)
    spark.udf.register("label", ((avr_month: Double, avr_total: Double) => if ((avr_month - avr_total) >= 0) 1.0 else 0.0))

    // 원본데이터
    // http://data.seoul.go.kr/openinf/sheetview.jsp?infId=OA-2604
    // 서울시 도시고속도로 월간 소통 통계
    val d1 = spark.read.option("header", "true")
      .option("sep", ",").option("inferSchema", true)
      .option("mode", "DROPMALFORMED")
      .csv("/Users/beginspark/Temp/data2.csv")

    val d2 = d1.toDF("year", "month", "road", "avr_traffic_month", "avr_velo_month", "mon", "tue", "wed", "thu", "fri", "sat", "sun")

    // data 확인
    d2.printSchema()

    // null 값 제거
    val d3 = d2.where("avr_velo_month is not null")

    // 도로별 평균 속도
    val d4 = d3.groupBy("road").agg(round(avg("avr_velo_month"), 1).as("avr_velo_total"))
    val d5 = d3.join(d4, Seq("road"))

    // label 부여
    val d6 = d5.withColumn("label", callUDF("label", $"avr_velo_month", $"avr_velo_total"))
    d6.select("road", "avr_velo_month", "avr_velo_total", "label").show(5, false)
    d6.groupBy("label").count().show(false)

    val Array(train, test) = d6.randomSplit(Array(0.7, 0.3))

    val indexer = new StringIndexer().setInputCol("road").setOutputCol("roadcode")

    val assembler = new VectorAssembler()
      .setInputCols(Array("roadcode", "mon", "tue", "wed", "thu", "fri", "sat", "sun"))
      .setOutputCol("features")

    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(indexer, assembler, dt))

    val model = pipeline.fit(train)

    val predict = model.transform(test)

    predict.select("label", "probability", "prediction").show(3, false)

    // areaUnderROC, areaUnderPR
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("areaUnderROC")

    println(evaluator.evaluate(predict))

    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)
  }
}
