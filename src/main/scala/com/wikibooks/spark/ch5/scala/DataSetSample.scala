package com.wikibooks.spark.ch5.scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}

object DatasetSample {

  // 5.6.1절
  def createDataSet(spark: SparkSession, sc: SparkContext) {

    import spark.implicits._

    val sparkHomeDir = "/Users/beginspark/Apps/spark"

    // 파일로 부터 생성
    val ds1 = spark.read.textFile(sparkHomeDir + "/examples/src/main/resources/people.txt")

    // 자바 객체를 이용해 생성
    val row1 = Person("hayoon", 7, "student")
    val row2 = Person("sunwoo", 13, "student")
    val row3 = Person("hajoo", 5, "kindergartener")
    val row4 = Person("jinwoo", 13, "student")
    val data = List(row1, row2, row3, row4)
    val ds2 = spark.createDataset(data)
    // 스칼라의 경우 암묵적 변환을 통한 toDS() 메서드를 사용할 수 있음
    val ds2_1 = data.toDS()

    // Encoder
    val e1 = Encoders.BOOLEAN
    val e2 = Encoders.LONG
    val e3 = Encoders.scalaBoolean
    val e5 = Encoders.scalaLong
    val e6 = Encoders.javaSerialization[Person]
    val e7 = Encoders.kryo[Person]
    val e8 = Encoders.tuple(Encoders.STRING, Encoders.INT)

    // RDD를 이용해 생성 (createDataset 또는 암묵적 변환을 통한 toDS 메서드 사용)
    val rdd = sc.parallelize(List(1, 2, 3))
    val ds3 = spark.createDataset(rdd)
    val ds4 = rdd.toDS()

    // 데이터프레임을 이용해 생성
    val ds5 = List(1, 2, 3).toDF.as[Int]
    // 데이터프레임을 생성하기 위해서는 scala.Product 하위타입의 요소로 구성된 RDD가 필요하므로
    // rdd.map(Tuple(_))을 통해 Tuple 타입으로 변환 후 createDataFrame 메서드를 사용 함
    val ds6 = spark.createDataFrame(rdd.map(Tuple1(_))).as[Int]

    // range()로 생성
    val ds7 = spark.range(0, 10, 3)

    // 결과 확인
    ds1.show
    ds2.show
    ds2_1.show
    ds3.show
    ds4.show
    ds5.show
    ds6.show
    ds7.show

    // 참고(Dataset을 데이터프레임 및 RDD로 변환)
    ds7.toDF()
    ds7.rdd
  }

  // 5.6.2.1절
  def runSelectEx(spark: SparkSession, ds: Dataset[Person]) {
    import spark.implicits._
    ds.select(ds("name").as[String], ds("age").as[Int]).show
  }

  // 5.6.2.2절
  def runAsEx(spark: SparkSession) {
    val d1 = spark.range(5, 15).as("First")
    val d2 = spark.range(10, 20).as("Second")
    d1.join(d2, expr("First.id = Second.id")).show
  }

  // 5.6.2.3절
  def runDistinctEx(spark: SparkSession) {
    import spark.implicits._
    List(1, 3, 3, 5, 5, 7).toDS.distinct().show
  }

  // 5.6.2.4절
  def runDropDuplicatesEx(spark: SparkSession, ds: Dataset[Person]) {
    // 원래 값
    ds.show
    // 중복을 제외한 후
    ds.dropDuplicates("age").show
  }

  // 5.6.2.5절
  def runFilterEx(spark: SparkSession, ds: Dataset[Person]) {
    ds.filter(_.age < 10).show
  }

  // 5.6.2.6절
  def runFlatMapEx(spark: SparkSession) {
    import spark.implicits._
    val sentence = "Spark SQL, DataFrames and Datasets Guide)"
    List(sentence).toDS().flatMap(_.split(" ")).show
  }

  // 5.6.2.7절
  def runGroupByKey(spark: SparkSession, ds: Dataset[Person]): Unit = {
    import spark.implicits._
    ds.groupByKey(_.age).count().show(false)
  }

  // 5.6.2.8절
  def runAgg(spark: SparkSession, ds: Dataset[Person]): Unit = {
    import spark.implicits._
    ds.show
    ds.groupByKey(_.job).agg(max("age").as[Int], countDistinct("age").as[Long]).show
  }

  // 5.6.2.9절
  def runMapValueAndReduceGroups(spark: SparkSession, ds: Dataset[Person]): Unit = {
    import spark.implicits._
    val r1 = ds.groupByKey(_.job).mapValues(p => p.name + "(" + p.age + ") ")
    val r2 = r1.reduceGroups((s1, s2) => s1 + s2)
    r2.show(false)
  }

  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("DatasetSample")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    // 2. 컬렉션으로부터 생성
    val row1 = Person("hayoon", 7, "student")
    val row2 = Person("sunwoo", 13, "student")
    val row3 = Person("hajoo", 5, "kindergartener")
    val row4 = Person("jinwoo", 13, "student")
    val data = List(row1, row2, row3, row4)
    val ds = spark.createDataset(data)

    //createDataSet(spark, spark.sparkContext)
    //runSelectEx(spark, ds)
    //runAsEx(spark)
    //runDistinctEx(spark)
    //runDropDuplicatesEx(spark, ds)
    //runFilterEx(spark, ds)
    //runFlatMapEx(spark)
    //runGroupByKey(spark, ds)
    //runAgg(spark, ds)
    //runMapValueAndReduceGroups(spark, ds)

    spark.stop
  }
}
