package com.wikibooks.spark.ch8;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import static org.apache.spark.sql.functions.collect_set;

public class RegressionSample {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession.builder()
            .appName("RegressionSample")
            .master("local[*]")
            .getOrCreate();

    // 데이터 제공처 : 서울시 학생 체격 현황 (키, 몸무게) 통계
    // (http://data.seoul.go.kr/openinf/linkview.jsp?infId=OA-12381&tMenu=11)
    Dataset<Row> df1 = spark.read().option("header", "false")
            .option("sep", "\t")
            .option("inferSchema", true)
            .csv("/Users/beginspark/Temp/Octagon.txt");


    df1.printSchema();
    df1.show(5, false);

    // Header 제거
    Dataset<Row> df2 = df1.where(df1.col("_c0").$eq$bang$eq("기간"));
    df2.show(3, false);

    spark.udf().register("toDouble", new UDF1<String, Double>() {
      @Override
      public Double call(String s) throws Exception {
        return Double.parseDouble(s.replaceAll("[^0-9.]", ""));
      }
    }, DataTypes.DoubleType);

    // cache
    df2.cache();

    // 초등학교 남 키, 몸무게
    Dataset<Row> df3 = df2.select(df2.col("_c0").as("year"),
            callUDF("toDouble", df2.col("_c2")).as("height"),
            callUDF("toDouble", df2.col("_c4")).as("weight"))
            .withColumn("grade", lit("elementary"))
            .withColumn("gender", lit("man"));

    // 초등학교 여 키, 몸무게
    Dataset<Row> df4 = df2.select(df2.col("_c0").as("year"),
            callUDF("toDouble", df2.col("_c3")).as("height"),
            callUDF("toDouble", df2.col("_c5")).as("weight"))
            .withColumn("grade", lit("elementary"))
            .withColumn("gender", lit("woman"));

    // 중학교 남 키, 몸무게
    Dataset<Row> df5 = df2.select(df2.col("_c0").as("year"),
            callUDF("toDouble", df2.col("_c6")).as("height"),
            callUDF("toDouble", df2.col("_c8")).as("weight"))
            .withColumn("grade", lit("middle"))
            .withColumn("gender", lit("man"));

    // 중학교 여 키, 몸무게
    Dataset<Row> df6 = df2.select(df2.col("_c0").as("year"),
            callUDF("toDouble", df2.col("_c7")).as("height"),
            callUDF("toDouble", df2.col("_c9")).as("weight"))
            .withColumn("grade", lit("middle"))
            .withColumn("gender", lit("woman"));

    // 고등학교 남 키, 몸무게
    Dataset<Row> df7 = df2.select(df2.col("_c0").as("year"),
            callUDF("toDouble", df2.col("_c10")).as("height"),
            callUDF("toDouble", df2.col("_c12")).as("weight"))
            .withColumn("grade", lit("high"))
            .withColumn("gender", lit("man"));

    // 고등학교 여 키, 몸무게
    Dataset<Row> df8 = df2.select(df2.col("_c0").as("year"),
            callUDF("toDouble", df2.col("_c11")).as("height"),
            callUDF("toDouble", df2.col("_c13")).as("weight"))
            .withColumn("grade", lit("high"))
            .withColumn("gender", lit("woman"));

    Dataset<Row> df9 = df3.union(df4).union(df5).union(df6).union(df7).union(df8);

    // 연도, 키, 몸무게, 학년, 성별
    df9.show(5, false);
    df9.printSchema();

    // 문자열 컬럼을 double로 변환
    StringIndexer gradeIndexer = new StringIndexer()
            .setInputCol("grade")
            .setOutputCol("gradecode");

    StringIndexer genderIndexer = new StringIndexer()
            .setInputCol("gender")
            .setOutputCol("gendercode");

    Dataset<Row> df10 = gradeIndexer.fit(df9).transform(df9);
    Dataset<Row> df11 = genderIndexer.fit(df10).transform(df10);

    df11.show(3, false);

    VectorAssembler assembler = new VectorAssembler()
            .setInputCols(new String[]{"height", "gradecode", "gendercode"})
            .setOutputCol("features");

    Dataset<Row> df12 = assembler.transform(df11);

    df12.show(false);

    Dataset<Row>[] dataArr = df12.randomSplit(new double[]{0.7, 0.3});
    Dataset<Row> training = dataArr[0];
    Dataset<Row> test = dataArr[1];

    LinearRegression lr = new LinearRegression()
            .setMaxIter(5)
            .setRegParam(0.3)
            .setLabelCol("weight")
            .setFeaturesCol("features");

    LinearRegressionModel model = lr.fit(training);

    System.out.println("결정계수(R2):" + model.summary().r2());

    Dataset<Row> d13 = model.setPredictionCol("predic_weight").transform(test);
    d13.cache();

    d13.select("weight", "predic_weight").show(5, false);

    RegressionEvaluator evaluator = new RegressionEvaluator();
    evaluator.setLabelCol("weight").setPredictionCol("predic_weight");

    // root mean squared error
    double rmse = evaluator.evaluate(d13);
    // mean squared error
    double mse = evaluator.setMetricName("mse").evaluate(d13);
    // R2 metric
    double r2 = evaluator.setMetricName("r2").evaluate(d13);
    // mean absolute error
    double mae = evaluator.setMetricName("mae").evaluate(d13);

    System.out.println("rmse:" + rmse + ", mse:" + mse + ", r2:" + r2 + ", mae:" + mae);

    // 파이프라인
    Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{gradeIndexer, genderIndexer, assembler, lr});

    Dataset<Row>[] dataArr2 = df9.randomSplit(new double[]{0.7, 0.3});
    Dataset<Row> training2 = dataArr2[0];
    Dataset<Row> test2 = dataArr2[1];

    // 파이프라인 모델 생성
    PipelineModel pipelineModel = pipeline.fit(training2);

    // 파이프라인 모델을 이용한 예측값 생성
    pipelineModel.transform(test2)
            .select("weight", "prediction").show(5, false);

    spark.stop();
  }
}













