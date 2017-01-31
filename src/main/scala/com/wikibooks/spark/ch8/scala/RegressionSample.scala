package com.wikibooks.spark.ch8.scala

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{DecisionTreeRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.sql.SparkSession

object RegressionSample {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("RegressionSample")
      .master("local[*]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 데이터 제공처 : 서울시 학생 체격 현황 (키, 몸무게) 통계
    // (http://data.seoul.go.kr/openinf/linkview.jsp?infId=OA-12381&tMenu=11)
    val df1 = spark.read.option("header", "false")
      .option("sep", "\t")
      .option("inferSchema", true)
      .csv("/Users/beginspark/Temp/Octagon.txt")

    df1.printSchema()
    df1.show(5, false)

    // Header 제거
    val df2 = df1.where(df1("_c0") =!= "기간")
    df2.show(3, false)

    spark.udf.register("toDouble", (v: String) => {
      v.replaceAll("[^0-9.]", "").toDouble
    })

    // cache
    df2.cache()

    // 초등학교 남 키, 몸무게
    val df3 = df2.select('_c0.as("year"),
      callUDF("toDouble", '_c2).as("height"),
      callUDF("toDouble", '_c4).as("weight"))
      .withColumn("grade", lit("elementary"))
      .withColumn("gender", lit("man"))

    // 초등학교 여 키, 몸무게
    val df4 = df2.select('_c0.as("year"),
      callUDF("toDouble", '_c3).as("height"),
      callUDF("toDouble", '_c5).as("weight"))
      .withColumn("grade", lit("elementary"))
      .withColumn("gender", lit("woman"))

    // 중학교 남 키, 몸무게
    val df5 = df2.select('_c0.as("year"),
      callUDF("toDouble", '_c6).as("height"),
      callUDF("toDouble", '_c8).as("weight"))
      .withColumn("grade", lit("middle"))
      .withColumn("gender", lit("man"))

    // 중학교 여 키, 몸무게
    val df6 = df2.select('_c0.as("year"),
      callUDF("toDouble", '_c7).as("height"),
      callUDF("toDouble", '_c9).as("weight"))
      .withColumn("grade", lit("middle"))
      .withColumn("gender", lit("woman"))

    // 고등학교 남 키, 몸무게
    val df7 = df2.select('_c0.as("year"),
      callUDF("toDouble", '_c10).as("height"),
      callUDF("toDouble", '_c12).as("weight"))
      .withColumn("grade", lit("high"))
      .withColumn("gender", lit("man"))

    // 고등학교 여 키, 몸무게
    val df8 = df2.select('_c0.as("year"),
      callUDF("toDouble", '_c11).as("height"),
      callUDF("toDouble", '_c13).as("weight"))
      .withColumn("grade", lit("high"))
      .withColumn("gender", lit("woman"))

    val df9 = df3.union(df4).union(df5).union(df6).union(df7).union(df8)

    // 연도, 키, 몸무게, 학년, 성별
    df9.show(5, false)
    df9.printSchema()

    // 문자열 컬럼을 double로 변환
    val gradeIndexer = new StringIndexer()
      .setInputCol("grade")
      .setOutputCol("gradecode")

    val genderIndexer = new StringIndexer()
      .setInputCol("gender")
      .setOutputCol("gendercode")

    val df10 = gradeIndexer.fit(df9).transform(df9)
    val df11 = genderIndexer.fit(df10).transform(df10)

    df11.show(3, false)

    val assembler = new VectorAssembler()
      .setInputCols(Array("height", "gradecode", "gendercode"))
      .setOutputCol("features")

    val df12 = assembler.transform(df11)

    df12.show(false)

    val Array(training, test) = df12.randomSplit(Array(0.7, 0.3))

    val lr = new LinearRegression()
      .setMaxIter(5)
      .setRegParam(0.3)
      .setLabelCol("weight")
      .setFeaturesCol("features")

    val model = lr.fit(training)

    println("결정계수(R2):" + model.summary.r2)

    val d13 = model.setPredictionCol("predic_weight").transform(test)
    d13.cache()

    d13.select("weight", "predic_weight").show(5, false)

    val evaluator = new RegressionEvaluator()
    evaluator.setLabelCol("weight").setPredictionCol("predic_weight")

    // root mean squared error
    val rmse = evaluator.evaluate(d13)
    // mean squared error
    val mse = evaluator.setMetricName("mse").evaluate(d13)
    // R2 metric
    val r2 = evaluator.setMetricName("r2").evaluate(d13)
    // mean absolute error
    val mae = evaluator.setMetricName("mae").evaluate(d13)

    println(s"rmse:${rmse}, mse:${mse}, r2:${r2}, mae:${mae}")

    // 파이프라인
    val pipeline = new Pipeline().setStages(Array(gradeIndexer, genderIndexer, assembler, lr))
    val Array(training2, test2) = df9.randomSplit(Array(0.7, 0.3))
    // 파이프라인 모델 생성
    val pipelineModel = pipeline.fit(training2)
    // 파이프라인 모델을 이용한 예측값 생성
    pipelineModel.transform(test2)
      .select("weight", "prediction").show(5, false)

    spark.stop
  }
}