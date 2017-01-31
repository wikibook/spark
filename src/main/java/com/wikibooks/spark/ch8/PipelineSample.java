package com.wikibooks.spark.ch8;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class PipelineSample {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession.builder()
            .appName("PipelineSample")
            .master("local[*]")
            .getOrCreate();

    StructField sf1 = DataTypes.createStructField("height", DataTypes.DoubleType, true);
    StructField sf2 = DataTypes.createStructField("weight", DataTypes.DoubleType, true);
    StructField sf3 = DataTypes.createStructField("age", DataTypes.IntegerType, true);
    StructField sf4 = DataTypes.createStructField("label", DataTypes.DoubleType, true);
    StructType schema1 = DataTypes.createStructType(Arrays.asList(sf1, sf2, sf3, sf4));

    List<Row> rows1 = Arrays.asList(RowFactory.create(161.0, 69.87, 29, 1.0),
            RowFactory.create(176.78, 74.35, 34, 1.0),
            RowFactory.create(159.23, 58.32, 29, 0.0));

    // 훈련용 데이터 (키, 몸무게, 나이, 성별)
    Dataset<Row> training = spark.createDataFrame(rows1, schema1);

    training.cache();

    List<Row> rows2 = Arrays.asList(RowFactory.create(169.4, 75.3, 42),
            RowFactory.create(185.1, 85.0, 37),
            RowFactory.create(161.6, 61.2, 28));

    StructType schema2 = DataTypes.createStructType(Arrays.asList(sf1, sf2, sf3));

    // 테스트용 데이터
    Dataset<Row> test = spark.createDataFrame(rows2, schema2);

    training.show(false);

    VectorAssembler assembler = new VectorAssembler();
    assembler.setInputCols(new String[]{"height", "weight", "age"});
    assembler.setOutputCol("features");

    Dataset<Row> assembled_training = assembler.transform(training);

    assembled_training.show(false);

    // 모델 생성 알고리즘 (로지스틱 회귀 평가자)
    LogisticRegression lr = new LogisticRegression();
    lr.setMaxIter(10).setRegParam(0.01);

    // 모델 생성
    LogisticRegressionModel model = lr.fit(assembled_training);

    // 예측값 생성
    model.transform(assembled_training).show();

    // 파이프라인
    Pipeline pipeline = new Pipeline();
    pipeline.setStages(new PipelineStage[]{assembler, lr});

    // 파이프라인 모델 생성
    PipelineModel pipelineModel = pipeline.fit(training);

    // 파이프라인 모델을 이용한 예측값 생성
    pipelineModel.transform(training).show();

    String path1 = "/Users/beginspark/Temp/regression-model";
    String path2 = "/Users/beginspark/Temp/pipelinemodel";

    // 모델 저장
    model.write().overwrite().save(path1);
    pipelineModel.write().overwrite().save(path2);

    // 저장된 모델 불러오기
    LogisticRegressionModel loadedModel = LogisticRegressionModel.load(path1);
    PipelineModel loadedPipelineModel = PipelineModel.load(path2);

    spark.stop();
  }
}
