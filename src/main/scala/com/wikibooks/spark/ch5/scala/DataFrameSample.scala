package com.wikibooks.spark.ch5.scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object DataFrameSample {

  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("DataFrameSample")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    // sample dataframe 1
    val row1 = Person("hayoon", 7, "student")
    val row2 = Person("sunwoo", 13, "student")
    val row3 = Person("hajoo", 5, "kindergartener")
    val row4 = Person("jinwoo", 13, "student")
    val data = List(row1, row2, row3, row4)
    val sampleDf = spark.createDataFrame(data)

    val d1 = ("store2", "note", 20, 2000)
    val d2 = ("store2", "bag", 10, 5000)
    val d3 = ("store1", "note", 15, 1000)
    val d4 = ("store1", "pen", 20, 5000)
    val sampleDF2 = Seq(d1, d2, d3, d4).toDF("store", "product", "amount", "price")

    val ldf = Seq(Word("w1", 1), Word("w2", 1)).toDF
    val rdf = Seq(Word("w1", 2), Word("w3", 1)).toDF

    // [예제 실행 방법] 아래에서 원하는 예제의 주석을 제거하고 실행!!

    //createDataFrame(spark, spark.sparkContext)
    //runBasicOpsEx(spark, sc, sampleDf)
    //runColumnEx(spark, sc, sampleDf)
    //runAlias(spark, sc, sampleDf)
    //runIsinEx(spark, sc)
    //runWhenEx(spark, sc)
    //runMaxMin(spark, sampleDf)
    //runAggregateFunctions(spark, sampleDf, sampleDF2)
    //runCollectionFunctions(spark)
    //runDateFunctions(spark)
    //runMathFunctions(spark)
    //runOtherFunctions(spark, sampleDf)
    //runUDF(spark, sampleDf)
    //runRowEx(spark)
    //runAgg(spark, sampleDF2)
    //runDfAlias(spark, sampleDF2)
    //runGroupBy(spark, sampleDF2)
    //runCube(spark, sampleDF2)
    //runDistinct(spark)
    //runDrop(spark, sampleDF2)
    //runIntersect(spark)
    //runExcept(spark)
    //runJoin(spark, ldf, rdf)
    //runNa(spark, ldf, rdf)
    //runOrderBy(spark)
    //runRollup(spark, sampleDF2)
    //runStat(spark)
    //runWithColumn(spark)
    //runSave(spark)

    spark.stop
  }

  def createDataFrame(spark: SparkSession, sc: SparkContext) {

    import spark.implicits._

    val sparkHomeDir = "/Users/beginspark/Apps/spark"

    // 1. 파일로 부터 생성     
    val df1 = spark.read.json(sparkHomeDir + "/examples/src/main/resources/people.json")
    val df2 = spark.read.parquet(sparkHomeDir + "/examples/src/main/resources/users.parquet")
    val df3 = spark.read.text(sparkHomeDir + "/examples/src/main/resources/people.txt")

    // 2. 컬렉션으로부터 생성 (ex5-15)
    val row1 = Person("hayoon", 7, "student")
    val row2 = Person("sunwoo", 13, "student")
    val row3 = Person("hajoo", 5, "kindergartener")
    val row4 = Person("jinwoo", 13, "student")
    val data = List(row1, row2, row3, row4)
    val df4 = spark.createDataFrame(data)
    //df4.show

    val df5 = data.toDF

    // 3. RDD로부터 생성 (ex5-18)
    val rdd = sc.parallelize(data)
    val df6 = spark.createDataFrame(rdd)
    val df7 = rdd.toDF

    // 4. 스키마 지정 (ex5-21)
    val sf1 = StructField("name", StringType, nullable = true)
    val sf2 = StructField("age", IntegerType, nullable = true)
    val sf3 = StructField("job", StringType, nullable = true)
    val schema = StructType(List(sf1, sf2, sf3))
    val rows = sc.parallelize(List(Row("hayoon", 7, "student"), Row("sunwoo", 13, "student"),
      Row("hajoo", 5, "kindergartener"), Row("jinwoo", 13, "student")))
    val df8 = spark.createDataFrame(rows, schema)
  }

  // 5.5.2.1.1절 ~ 5.5.2.2.4절
  def runBasicOpsEx(spark: SparkSession, sc: SparkContext, df: DataFrame) {
    df.show
    df.head
    df.first
    df.take(2)
    df.count
    df.collect
    df.collectAsList
    df.describe("age").show
    df.persist(StorageLevel.MEMORY_AND_DISK_2)
    df.printSchema()
    df.columns
    df.dtypes
    df.schema
    df.createOrReplaceTempView("users")
    spark.sql("select name, age from users where age > 20").show
    spark.sql("select name, age from users where age > 20").explain
  }

  // 5.5.2.4절
  def runColumnEx(spark: SparkSession, sc: SparkContext, df: DataFrame) {

    import spark.implicits._

    // age > 10인 데이터만 조회 
    df.createOrReplaceTempView("person")
    spark.sql(" select * from person where age > 10 ").show

    // Column을 생성하는 다양한 방법   
    df.where(col("age") > 10).show
    df.where(df("age") > 10).show
    df.where('age > 10).show
    df.where($"age" > 10).show
  }

  // 5.5.2.4.2절
  def runAlias(spark: SparkSession, sc: SparkContext, df: DataFrame) {
    import spark.implicits._

    df.select('age + 1).show()
    df.select(('age + 1).as("age")).show()

    // note
    val df1 = List(MyCls("id1", Map("key1" -> "value1", "key2" -> "value2"))).toDF("id", "value")
    // alias
    val df2 = df1.select(explode($"value").as(List("key", "value"))).show
    // alias with meta info
    val meta = new MetadataBuilder().putString("desc", "this is the first column").build()
    val df3 = df1.select(col("id").as("idm", meta))
    df3.schema.fields.foreach { sf => println(sf.metadata) }
  }

  // 5.5.2.4.3절
  def runIsinEx(spark: SparkSession, sc: SparkContext) {
    val nums = sc.broadcast(List(1, 3, 5, 7, 9))
    val ds = spark.range(0, 10)
    val col = ds("id").isin(nums.value: _*)
    ds.where(col).show
  }

  // 5.5.2.4.4절
  def runWhenEx(spark: SparkSession, sc: SparkContext) {
    val ds = spark.range(0, 5)
    val col = when(ds("id") % 2 === 0, "even").otherwise("odd").as("type")
    ds.select(ds("id"), col).show()
  }

  // 5.5.2.4.5절
  def runMaxMin(spark: SparkSession, df: DataFrame) {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    df.select(min('age), mean('age)).show
  }

  // 5.5.2.4.6절 ~ 5.5.2.4.9절
  def runAggregateFunctions(spark: SparkSession, df1: DataFrame, df2: DataFrame) {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // collect_list, collect_set
    val doubledDf1 = df1.union(df1)
    doubledDf1.select(collect_list("name")).show(false)
    doubledDf1.select(collect_set("name")).show(false)

    // count, countDistinct
    doubledDf1.select(count("name"), countDistinct("name")).show(false)

    // sum
    df2.select(sum("price")).show(false)

    // grouping, grouping_id
    df2.cube('store, 'product).agg(sum("amount"), grouping("store")).show
    df2.cube('store, 'product).agg(sum("amount"), grouping_id("store", "product")).show
  }

  // 5.5.2.4.10 ~ 5.5.2.4.11 절
  def runCollectionFunctions(spark: SparkSession) {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df = Seq(Array(9, 1, 5, 3, 9)).toDF("array")

    // array_contains, size
    df.select('array, array_contains('array, 2), size('array)).show(false)

    // sort_array
    df.select('array, sort_array('array)).show(false)

    // explode, posexplode
    df.select(explode('array)).show(false)
    df.select(posexplode('array)).show(false)
  }

  // 5.5.2.4.12 ~ 5.5.2.4.14절
  def runDateFunctions(spark: SparkSession) {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val date1 = "2017-12-25 12:00:05"
    val date2 = "2017-12-25"

    val df = Seq((date1, date2)).toDF("d1", "d2")
    df.show(false)

    // current_date, unix_timestamp, to_date
    val d3 = current_date().as("d3")
    val d4 = unix_timestamp(df("d1")).as("d4")
    val d5 = to_date(df("d2")).as("d5")
    val d6 = to_date(d4.cast("timestamp")).as("d6")
    df.select('d1, 'd2, d3, d4, d5, d6).show

    // add_months, date_add, last_day
    val d7 = add_months(d6, 2).as("d7")
    val d8 = date_add(d6, 2).as("d8")
    val d9 = last_day(d6).as("d9")
    df.select('d1, 'd2, d7, d8, d9).show

    // window
    val p1 = ("2017-12-25 12:01:00", "note", 1000)
    val p2 = ("2017-12-25 12:01:10", "pencil", 3500)
    val p3 = ("2017-12-25 12:03:20", "pencil", 23000)
    val p4 = ("2017-12-25 12:05:00", "note", 1500)
    val p5 = ("2017-12-25 12:05:07", "note", 2000)
    val p6 = ("2017-12-25 12:06:25", "note", 1000)
    val p7 = ("2017-12-25 12:08:00", "pencil", 500)
    val p8 = ("2017-12-25 12:09:45", "note", 30000)

    val dd = Seq(p1, p2, p3, p4, p5, p6, p7, p8).toDF("date", "product", "amount")
    dd.groupBy(window(unix_timestamp('date).cast("timestamp"), "5 minutes"), 'product).agg(sum('amount)).show(false)
  }

  // 5.5.2.4.15절
  def runMathFunctions(spark: SparkSession) {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    Seq(1.512, 2.234, 3.42).toDF("value").select(round('value, 1)).show
    Seq(25, 9, 10).toDF("value").select(sqrt('value)).show
  }

  // 5.5.2.4.16 ~ 5.5.2.4.20절
  def runOtherFunctions(spark: SparkSession, personDf: DataFrame) {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df = Seq(("v1", "v2", "v3")).toDF("c1", "c2", "c3")

    // array
    df.select($"c1", $"c2", $"c3", array("c1", "c2", "c3").as("newCol")).show(false)

    // desc, asc
    personDf.show
    personDf.sort(desc("age"), asc("name")).show(false)

    // desc_nulls_first, desc_nulls_last, asc_nulls_first, asc_nulls_last
    val df2 = Seq(("r11", "r12", "r13"), ("r21", "r22", null), ("r31", "r32", "r33")).toDF("c1", "c2", "c3")
    df2.show
    df2.sort(asc_nulls_first("c3")).show

    // split, length
    Seq(("Splits str around pattern")).toDF("value").select('value, split('value, " "), length('value)).show(false)

    // rownum, rank
    val p1 = ("2017-12-25 12:01:00", "note", 1000)
    val p2 = ("2017-12-25 12:01:10", "pencil", 3500)
    val p3 = ("2017-12-25 12:03:20", "pencil", 23000)
    val p4 = ("2017-12-25 12:05:00", "note", 1500)
    val p5 = ("2017-12-25 12:05:07", "note", 2000)
    val p6 = ("2017-12-25 12:06:25", "note", 1000)
    val p7 = ("2017-12-25 12:08:00", "pencil", 500)
    val p8 = ("2017-12-25 12:09:45", "note", 30000)
    val dd = Seq(p1, p2, p3, p4, p5, p6, p7, p8).toDF("date", "product", "amount")
    val w1 = Window.partitionBy("product").orderBy("amount")
    val w2 = Window.orderBy("amount")
    dd.select('product, 'amount, row_number().over(w1).as("rownum"), rank().over(w2).as("rank")).show
  }

  // 5.5.2.4.21절
  def runUDF(spark: SparkSession, df: DataFrame) {
    import spark.implicits._

    // functions를 이용한 등록
    val fn1 = udf((job: String) => job match {
      case "student" => true
      case _ => false
    })

    df.select('name, 'age, 'job, fn1('job)).show

    // SparkSession을 이용한 등록
    spark.udf.register("fn2", (job: String) => job match {
      case "student" => true
      case _ => false
    })

    df.createOrReplaceTempView("persons")
    spark.sql("select name, age, job, fn2(job) from persons").show
  }

  // 스칼라의 경우 Row에 대한 패턴 매치가 가능
  def runRowEx(spark: SparkSession) {
    import spark.implicits._
    val df = Seq(("r1", 1), ("r2", 2), ("r3", 3)).toDF
    df.foreach { row =>
      row match {
        case Row(col1, col2) => println(s"col1:${col1}, col2:${col2}")
      }
    }
  }

  // 5.5.2.4.24절
  def runAgg(spark: SparkSession, df: DataFrame) {
    import org.apache.spark.sql.functions._
    df.agg(max("amount"), min("price")).show
    df.agg(Map("amount" -> "max", "price" -> "min")).show
  }

  // 5.5.2.4.26절
  def runDfAlias(spark: SparkSession, df: DataFrame) {
    df.select(df("product")).show
    df.alias("aa").select("aa.product").show
  }

  // 5.5.2.4.27절
  def runGroupBy(spark: SparkSession, df: DataFrame) {
    df.groupBy("store", "product").agg("price" -> "sum").show
  }

  // 5.5.3.4.28절
  def runCube(spark: SparkSession, df: DataFrame) {
    df.cube("store", "product").agg("price" -> "sum").show
  }

  // 5.5.2.4.29절
  def runDistinct(spark: SparkSession) {
    import spark.implicits._
    val d1 = ("store1", "note", 20, 2000)
    val d2 = ("store1", "bag", 10, 5000)
    val d3 = ("store1", "note", 20, 2000)
    val df = Seq(d1, d2, d3).toDF("store", "product", "amount", "price")
    df.distinct.show
    df.dropDuplicates("store").show
  }

  // 5.5.2.4.30절
  def runDrop(spark: SparkSession, df: DataFrame) {
    import spark.implicits._
    df.drop('store).show
  }

  // 5.5.2.4.31절
  def runIntersect(spark: SparkSession) {
    val a = spark.range(1, 5).toDF
    val b = spark.range(2, 6).toDF
    val c = a.intersect(b)
    c.show
  }

  // 5.5.2.4.32절
  def runExcept(spark: SparkSession) {
    import spark.implicits._
    val df1 = List(1, 2, 3, 4, 5).toDF
    val df2 = List(2, 4).toDF
    df1.except(df2).show
  }

  // 5.5.2.4.33절
  def runJoin(spark: SparkSession, ldf: DataFrame, rdf: DataFrame) {
    val joinTypes = "inner,outer,leftouter,rightouter,leftsemi".split(",")
    joinTypes.foreach((joinType: String) => {
      println(s"============= ${joinType} ===============")
      ldf.join(rdf, Seq("word"), joinType).show
    })
  }

  // 5.5.2.4.35절
  def runNa(spark: SparkSession, ldf: DataFrame, rdf: DataFrame) {
    val result = ldf.join(rdf, Seq("word"), "outer").toDF("word", "c1", "c2")
    result.show
    result.na.drop(2, Seq("c1", "c2")).show
    result.na.fill(Map("c1" -> 0)).show
    result.na.replace("word", Map("w1" -> "word1", "w2" -> "word2")).show
  }

  // 5.5.2.4.36절
  def runOrderBy(spark: SparkSession) {
    import spark.implicits._
    val df = List((3, "z"), (10, "a"), (5, "c")).toDF("idx", "name")
    df.orderBy("name", "idx").show
    df.orderBy("idx", "name").show
  }

  // 5.5.2.4.37절
  def runRollup(spark: SparkSession, df: DataFrame) {
    df.rollup("store", "product").agg("price" -> "sum").show
  }

  // 5.5.2.4.38절
  def runStat(spark: SparkSession) {
    import spark.implicits._
    val df = List(("a", 6), ("b", 4), ("c", 12), ("d", 6)).toDF("word", "count")
    df.show
    df.stat.crosstab("word", "count").show
  }

  // 5.5.2.4.39절
  def runWithColumn(spark: SparkSession) {
    import spark.implicits._
    val df1 = List(("prod1", "100"), ("prod2", "200")).toDF("pname", "price")
    val df2 = df1.withColumn("dcprice", 'price * 0.9)
    val df3 = df2.withColumnRenamed("dcprice", "newprice")
    df1.show
    df2.show
    df3.show
  }

  // 5.5.2.4.40절
  def runSave(spark: SparkSession) {
    val sparkHomeDir = "/Users/beginspark/Apps/spark"
    val df = spark.read.json(sparkHomeDir + "/examples/src/main/resources/people.json")
    df.write.save("/Users/beginspark/Temp/default/" + System.currentTimeMillis())
    df.write.format("json").save("/Users/beginspark/Temp/json/" + System.currentTimeMillis())
    df.write.format("json").partitionBy("age").save("/Users/beginspark/Temp/parti/" + System.currentTimeMillis())
    df.write.mode(SaveMode.Overwrite).saveAsTable("ohMyTable")
    spark.sql("select * from ohMyTable").show
    // bucketBy의 경우 테이블로 저장해야 햠
    df.write.format("json").bucketBy(20, "age").mode(SaveMode.Overwrite).saveAsTable("ohMyBuckedTable")
    spark.sql("select * from ohMyBuckedTable").show
  }
}
