package com.wikibooks.spark.ch5;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class DataFrameSample {

  public static void main(String[] args) {

    SparkSession spark = SparkSession
            .builder()
            .appName("DataFrameSample")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate();

    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

    // sample dataframe(dataset<Row>) 1
    Person row1 = new Person("hayoon", 7, "student");
    Person row2 = new Person("sunwoo", 13, "student");
    Person row3 = new Person("hajoo", 5, "kindergartener");
    Person row4 = new Person("jinwoo", 13, "student");

    List<Person> data = Arrays.asList(row1, row2, row3, row4);
    Dataset<Row> sampleDf = spark.createDataFrame(data, Person.class);

    Product d1 = new Product("store2", "note", 20, 2000);
    Product d2 = new Product("store2", "bag", 10, 5000);
    Product d3 = new Product("store1", "note", 15, 1000);
    Product d4 = new Product("store1", "pen", 20, 5000);
    Dataset<Row> sampleDF2 = spark.createDataFrame(Arrays.asList(d1, d2, d3, d4), Product.class);

    Dataset<Row> ldf = spark.createDataFrame(Arrays.asList(new Word("w1", 1), new Word("w2", 1)), Word.class);
    Dataset<Row> rdf = spark.createDataFrame(Arrays.asList(new Word("w1", 2), new Word("w3", 1)), Word.class);

    createDataFrame(spark, new JavaSparkContext(spark.sparkContext()));

    // [예제 실행 방법] 아래에서 원하는 예제의 주석을 제거하고 실행!!

    // runBasicOpsEx(spark, sc, sampleDf);
    // runColumnEx(spark, sc, sampleDf);
    // runAliasEx(spark, sc, sampleDf);
    // runIsinEx(spark, sc);
    // runWhenEx(spark, sc);
    // runMaxMin(spark, sampleDf);
    // runAggregateFunctions(spark, sampleDf, sampleDF2);
    // runCollectionFunctions(spark);
    // runDateFunctions(spark);
    // runMathFunctions(spark);
    // runOtherFunctions(spark, sampleDf);
    // runUDF(spark, sampleDf);
    // runAgg(spark, sampleDF2);
    // runDfAlias(spark, sampleDF2);
    // runGroupBy(spark, sampleDF2);
    // runCube(spark, sampleDF2);
    // runDistinct(spark);
    // runDrop(spark, sampleDF2);
    // runIntersect(spark);
    // runExcept(spark);
    // runJoin(spark, ldf, rdf);
    // runNa(spark, ldf, rdf);
    // runOrderBy(spark);
    // runRollup(spark, sampleDF2);
    // runStat(spark);
    // runWithColumn(spark);
    // runSave(spark);

    spark.stop();
  }

  public static void createDataFrame(SparkSession spark, JavaSparkContext sc) {

    String sparkHomeDir = "/Users/beginspark/Apps/spark";

    // 1. 파일로 부터 생성
    Dataset<Row> df1 = spark.read().json(sparkHomeDir + "/examples/src/main/resources/people.json");
    Dataset<Row> df2 = spark.read().parquet(sparkHomeDir + "/examples/src/main/resources/users.parquet");
    Dataset<Row> df3 = spark.read().text(sparkHomeDir + "/examples/src/main/resources/people.txt");

    // 2. 컬렉션으로부터 생성 (ex5-16)
    Person row1 = new Person("hayoon", 7, "student");
    Person row2 = new Person("sunwoo", 13, "student");
    Person row3 = new Person("hajoo", 5, "kindergartener");
    Person row4 = new Person("jinwoo", 13, "student");

    List<Person> data = Arrays.asList(row1, row2, row3, row4);
    Dataset<Row> df4 = spark.createDataFrame(data, Person.class);

    // 3. RDD로부터 생성 (ex5-19)
    JavaRDD<Person> rdd = sc.parallelize(data);
    Dataset<Row> df5 = spark.createDataFrame(rdd, Person.class);

    // 4. 스키마 지정(ex5-22)
    org.apache.spark.sql.types.StructField sf1 = DataTypes.createStructField("name", DataTypes.StringType, true);
    org.apache.spark.sql.types.StructField sf2 = DataTypes.createStructField("age", DataTypes.IntegerType, true);
    org.apache.spark.sql.types.StructField sf3 = DataTypes.createStructField("job", DataTypes.StringType, true);
    StructType schema = DataTypes.createStructType(Arrays.asList(sf1, sf2, sf3));
    Row r1 = RowFactory.create("hayoon", 7, "student");
    Row r2 = RowFactory.create("sunwoo", 13, "student");
    Row r3 = RowFactory.create("hajoo", 5, "kindergartener");
    Row r4 = RowFactory.create("jinwoo", 13, "student");

    List<Row> rows = Arrays.asList(r1, r2, r3, r4);
    Dataset<Row> df6 = spark.createDataFrame(rows, schema);
  }

  // 5.5.2.1.1절 ~ 5.5.2.2.4절
  public static void runBasicOpsEx(SparkSession spark, JavaSparkContext sc, Dataset<Row> df) {
    df.show();
    df.head();
    df.first();
    df.take(2);
    df.count();
    df.collect();
    df.collectAsList();
    df.describe("age").show();
    df.persist(StorageLevel.MEMORY_AND_DISK_2());
    df.printSchema();
    df.columns();
    df.dtypes();
    df.schema();
    df.createOrReplaceTempView("users");
    spark.sql("select name, age from users where age > 20").show();
    spark.sql("select name, age from users where age > 20").explain();
  }

  // 5.5.2.4절
  public static void runColumnEx(SparkSession spark, JavaSparkContext sc, Dataset<Row> df) {
    // age > 10인 데이터만 조회
    df.createOrReplaceTempView("person");
    // sql
    spark.sql(" select * from person where age > 10 ").show();
    // api
    df.where(df.col("age").gt(10)).show();
  }

  // 5.5.2.4.2절
  public static void runAliasEx(SparkSession spark, JavaSparkContext sc, Dataset<Row> df) {
    df.select(df.col("age").plus(1)).show();
    df.select(df.col("age").plus(1).as("age")).show();
  }

  // 5.5.2.4.3절
  public static void runIsinEx(SparkSession spark, JavaSparkContext sc) {
    Broadcast<List<Integer>> nums = sc.broadcast(Arrays.asList(1, 3, 5, 7, 9));
    Dataset<Long> ds = spark.range(0, 10);
    ds.where(ds.col("id").isin(nums.value().toArray())).show();
  }

  // 5.5.2.4.4절
  public static void runWhenEx(SparkSession spark, JavaSparkContext sc) {
    Dataset<Long> ds = spark.range(0, 5);
    Column col = when(ds.col("id").divide(2).equalTo(0), "even").otherwise("add");
    ds.select(ds.col("id"), col.as("type")).show();
  }

  // 5.5.2.4.5절
  public static void runMaxMin(SparkSession spark, Dataset<Row> df) {
    Column minCol = min("age");
    Column maxCol = max("age");
    df.select(minCol, maxCol).show();
  }

  // 5.5.2.4.6절 ~ 5.5.2.4.9절
  public static void runAggregateFunctions(SparkSession spark, Dataset<Row> df1, Dataset<Row> df2) {
    Dataset<Row> doubledDf1 = df1.union(df1);
    doubledDf1.select(collect_list("name")).show(false);
    doubledDf1.select(collect_set("name")).show(false);

    // count, countDistinct
    doubledDf1.select(count("name"), countDistinct("name")).show(false);

    // sum
    df2.select(sum("price")).show(false);

    // grouping, grouping_id
    df2.cube(df2.col("store"), df2.col("product")).agg(sum("amount"), grouping("store")).show();

//    // JavaConversions 사용하여 처리할 경우
//    Seq<String> colNames = JavaConversions.asScalaBuffer(Arrays.asList("product")).toSeq();
//    df2.cube(df2.col("store"), df2.col("product")).agg(sum("amount"), grouping_id("store", colNames)).show();
  }

  // 5.5.2.4.10 ~ 5.5.2.4.11 절
  public static void runCollectionFunctions(SparkSession spark) {

    StructField f1 = DataTypes.createStructField("numbers", DataTypes.StringType, true);
    StructType schema = DataTypes.createStructType(Arrays.asList(f1));
    Row r1 = RowFactory.create("9,1,5,3,9");

    Dataset<Row> df = spark.createDataFrame(Arrays.asList(r1), schema);
    // split는 문자열을 지정한 delimiter로 분리하여 배열 타입의 컬럼을 생성하는 functions 메서드임(5.5.2.4.19절 참조)
    Column arrayCol = split(df.col("numbers"), ",").as("array");

    // array_contains, size
    df.select(arrayCol, array_contains(arrayCol, 2), size(arrayCol)).show(false);

    // sort_array()
    df.select(arrayCol, sort_array(arrayCol)).show(false);

    // explode, posexplode
    df.select(explode(arrayCol)).show(false);
    df.select(posexplode(arrayCol)).show(false);
  }

  // 5.5.2.4.12 ~ 5.5.2.4.14절
  public static void runDateFunctions(SparkSession spark) {

    StructField f1 = DataTypes.createStructField("d1", DataTypes.StringType, true);
    StructField f2 = DataTypes.createStructField("d2", DataTypes.StringType, true);
    StructType schema1 = DataTypes.createStructType(Arrays.asList(f1, f2));
    Row r1 = RowFactory.create("2017-12-25 12:00:05", "2017-12-25");

    Dataset<Row> df = spark.createDataFrame(Arrays.asList(r1), schema1);
    df.show(false);

    // current_date, unix_timestamp, to_date
    Column d3 = current_date().as("d3");
    Column d4 = unix_timestamp(df.col("d1")).as("d4");
    Column d5 = to_date(df.col("d2")).as("d5");
    Column d6 = to_date(d4.cast("timestamp")).as("d6");
    df.select(df.col("d1"), df.col("d2"), d3, d4, d5, d6).show(false);

    // add_months, date_add, last_day
    Column d7 = add_months(d6, 2).as("d7");
    Column d8 = date_add(d6, 2).as("d8");
    Column d9 = last_day(d6).as("d9");
    df.select(df.col("d1"), df.col("d2"), d7, d8, d9).show(false);

    // window
    StructField f3 = DataTypes.createStructField("date", DataTypes.StringType, true);
    StructField f4 = DataTypes.createStructField("product", DataTypes.StringType, true);
    StructField f5 = DataTypes.createStructField("amount", DataTypes.IntegerType, true);
    StructType schema2 = DataTypes.createStructType(Arrays.asList(f3, f4, f5));

    Row r2 = RowFactory.create("2017-12-25 12:01:00", "note", 1000);
    Row r3 = RowFactory.create("2017-12-25 12:01:10", "pencil", 3500);
    Row r4 = RowFactory.create("2017-12-25 12:03:20", "pencil", 23000);
    Row r5 = RowFactory.create("2017-12-25 12:05:00", "note", 1500);
    Row r6 = RowFactory.create("2017-12-25 12:05:07", "note", 2000);
    Row r7 = RowFactory.create("2017-12-25 12:06:25", "note", 1000);
    Row r8 = RowFactory.create("2017-12-25 12:08:00", "pencil", 500);
    Row r9 = RowFactory.create("2017-12-25 12:09:45", "note", 30000);

    Dataset<Row> dd = spark.createDataFrame(Arrays.asList(r2, r3, r4, r5, r6, r7, r8, r9), schema2);

    Column timeCol = unix_timestamp(dd.col("date")).cast("timestamp");
    Column windowCol = window(timeCol, "5 minutes");
    dd.groupBy(windowCol, dd.col("product")).agg(sum(dd.col("amount"))).show(false);
  }

  // 5.5.2.4.15절
  public static void runMathFunctions(SparkSession spark) {
    StructField f1 = DataTypes.createStructField("value", DataTypes.DoubleType, true);
    StructType schema1 = DataTypes.createStructType(Arrays.asList(f1));

    Row r1 = RowFactory.create(1.512);
    Row r2 = RowFactory.create(2.234);
    Row r3 = RowFactory.create(3.42);
    Row r4 = RowFactory.create(25.0);
    Row r5 = RowFactory.create(9.0);
    Row r6 = RowFactory.create(10.0);

    Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(r1, r2, r3), schema1);
    Dataset<Row> df2 = spark.createDataFrame(Arrays.asList(r4, r5, r6), schema1);

    df1.select(round(df1.col("value"), 1)).show();
    df2.select(sqrt("value")).show();
  }

  // 5.5.2.4.16 ~ 5.5.2.4.20
  public static void runOtherFunctions(SparkSession spark, Dataset<Row> personDf) {

    StructField f1 = DataTypes.createStructField("c1", DataTypes.StringType, true);
    StructField f2 = DataTypes.createStructField("c2", DataTypes.StringType, true);
    StructField f3 = DataTypes.createStructField("c3", DataTypes.StringType, true);

    StructType schema1 = DataTypes.createStructType(Arrays.asList(f1, f2, f3));
    Row r1 = RowFactory.create("v1", "v2", "v3");

    Dataset<Row> df = spark.createDataFrame(Arrays.asList(r1), schema1);

    // array
    df.select(df.col("c1"), df.col("c2"), df.col("c3"), array("c1", "c2", "c3").as("newCol")).show(false);

    // desc, asc
    personDf.show();
    personDf.sort(desc("age"), asc("name")).show(false);

    // desc_nulls_first, desc_nulls_last, asc_nulls_first, asc_nulls_last
    Row r2 = RowFactory.create("r11", "r12", "r13");
    Row r3 = RowFactory.create("r21", "r22", null);
    Row r4 = RowFactory.create("r31", "r32", "r33");
    Dataset<Row> df2 = spark.createDataFrame(Arrays.asList(r2, r3, r4), schema1);

    df2.show();
    df2.sort(asc_nulls_first("c3")).show();

    // split, length
    StructField f4 = DataTypes.createStructField("value", DataTypes.StringType, true);
    StructType schema2 = DataTypes.createStructType(Arrays.asList(f4));
    Row r5 = RowFactory.create("Splits str around pattern");
    Dataset<Row> df3 = spark.createDataFrame(Arrays.asList(r5), schema2);
    df3.select(df3.col("value"), split(df3.col("value"), " "), length(df3.col("value"))).show(false);

    // rownum, rank
    StructField f5 = DataTypes.createStructField("date", DataTypes.StringType, true);
    StructField f6 = DataTypes.createStructField("product", DataTypes.StringType, true);
    StructField f7 = DataTypes.createStructField("amount", DataTypes.IntegerType, true);
    StructType schema3 = DataTypes.createStructType(Arrays.asList(f5, f6, f7));

    Row r6 = RowFactory.create("2017-12-25 12:01:00", "note", 1000);
    Row r7 = RowFactory.create("2017-12-25 12:01:10", "pencil", 3500);
    Row r8 = RowFactory.create("2017-12-25 12:03:20", "pencil", 23000);
    Row r9 = RowFactory.create("2017-12-25 12:05:00", "note", 1500);
    Row r10 = RowFactory.create("2017-12-25 12:05:07", "note", 2000);
    Row r11 = RowFactory.create("2017-12-25 12:06:25", "note", 1000);
    Row r12 = RowFactory.create("2017-12-25 12:08:00", "pencil", 500);
    Row r13 = RowFactory.create("2017-12-25 12:09:45", "note", 30000);

    Dataset<Row> dd = spark.createDataFrame(Arrays.asList(r6, r7, r8, r9, r10, r11, r12, r13), schema3);

    WindowSpec w1 = Window.partitionBy("product").orderBy("amount");
    WindowSpec w2 = Window.orderBy("amount");
    dd.select(dd.col("product"), dd.col("amount"), row_number().over(w1).as("rownum"), rank().over(w2).as("rank")).show();
  }

  // 5.5.2.4.21절
  public static void runUDF(SparkSession spark, Dataset<Row> df) {

//    JavaConversion를 사용하여 처리할 경우
//    Function1 fnc = new AbstractFunction1<String, Boolean>() {
//      @Override
//      public Boolean apply(String job) {
//        return StringUtils.equals(job, "student");
//      }
//    };
//
//    UserDefinedFunction fn1 = functions.udf(fnc, DataTypes.BooleanType);
//    Seq<Column> cols = JavaConversions.asScalaBuffer(Arrays.asList(df.col("job"))).toSeq();
//    df.select(df.col("name"), df.col("age"), df.col("job"), fn1.apply(cols)).show(false);

    // Java의 경우 SparkSession을 이용하는 것이 더 적합함
    spark.udf().register("fn2", new UDF1<String, Boolean>() {
      @Override
      public Boolean call(String job) throws Exception {
        return StringUtils.equals(job, "student");
      }
    }, DataTypes.BooleanType);

    df.select(df.col("name"), df.col("age"), df.col("job"), callUDF("fn2", df.col("job"))).show();
    df.createOrReplaceTempView("persons");
    spark.sql("select name, age, job, fn2(job) from persons").show();
  }

  // 5.5.2.4.24절
  public static void runAgg(SparkSession spark, Dataset<Row> df) {
    df.show();
    df.agg(max("amount"), min("price")).show();
    Map<String, String> paramMap = new HashMap<>();
    paramMap.put("amount", "max");
    paramMap.put("price", "min");
    df.agg(paramMap).show();
  }

  // 5.5.2.4.26절
  public static void runDfAlias(SparkSession spark, Dataset<Row> df) {
    df.select(df.col("product")).show();
    df.alias("aa").select("aa.product").show();
  }

  // 5.5.2.4.27절
  public static void runGroupBy(SparkSession spark, Dataset<Row> df) {
    Map<String, String> paramMap = new HashMap<>();
    paramMap.put("price", "sum");
    df.groupBy("store", "product").agg(paramMap).show();
  }

  // 5.5.3.4.28절
  public static void runCube(SparkSession spark, Dataset<Row> df) {
    Map<String, String> paramMap = new HashMap<>();
    paramMap.put("price", "sum");
    df.cube("store", "product").agg(paramMap).show();
  }

  // 5.5.2.4.29절
  public static void runDistinct(SparkSession spark) {
    Product d1 = new Product("store1", "note", 20, 2000);
    Product d2 = new Product("store1", "bag", 10, 5000);
    Product d3 = new Product("store1", "note", 20, 2000);
    Dataset<Row> df = spark.createDataFrame(Arrays.asList(d1, d2, d3), Product.class);
    df.distinct().show();
    df.dropDuplicates("store").show();
  }

  // 5.5.2.4.30절
  public static void runDrop(SparkSession spark, Dataset<Row> df) {
    df.drop(df.col("store")).show();
  }

  // 5.5.2.4.31절
  public static void runIntersect(SparkSession spark) {
    Dataset<Row> a = spark.range(1, 5).toDF();
    Dataset<Row> b = spark.range(2, 6).toDF();
    Dataset<Row> c = a.intersect(b);
    c.show();
  }

  // 5.5.2.4.32절
  public static void runExcept(SparkSession spark) {
    Dataset<Row> df1 = spark.range(1, 5).toDF();
    Dataset<Row> df2 = spark.range(2, 4).toDF();
    df1.except(df2).show();
  }

  // 5.5.2.4.33절
  public static void runJoin(SparkSession spark, Dataset<Row> ldf, Dataset<Row> rdf) {
    for (String joinType : "inner,outer,leftouter,rightouter,leftsemi".split(",")) {
      System.out.println("joinType:" + joinType);
      ldf.join(rdf, ldf.col("word").equalTo(rdf.col("word")), joinType).show();

//      //JavaConversion를 사용하여 처리할 경우
//      Seq<String> joinCols = JavaConversions.asScalaBuffer(Arrays.asList("word")).toSeq();
//      ldf.join(rdf, joinCols, joinType).show();
    }
  }

  // 5.5.2.4.35절
  public static void runNa(SparkSession spark, Dataset<Row> ldf, Dataset<Row> rdf) {
    // drop
    ldf = ldf.toDF("c1", "lword");
    rdf = rdf.toDF("c2", "rword");
    Dataset<Row> result = ldf.join(rdf, ldf.col("lword").equalTo(rdf.col("rword")), "outer")
            .select(ldf.col("lword").as("word"), ldf.col("c1"), rdf.col("c2"));
    result.na().drop(2, new String[]{"c1", "c2"}).show();
    // fill
    Map<String, Object> fillMap1 = new HashMap<>();
    fillMap1.put("c1", 0);
    result.na().fill(fillMap1).show();
    // replace
    Map<String, String> fillMap2 = new HashMap<>();
    fillMap2.put("w1", "word1");
    fillMap2.put("w2", "word2");
    result.na().replace("word", fillMap2).show();
  }

  // 5.5.2.4.36절
  public static void runOrderBy(SparkSession spark) {

    StructField f1 = DataTypes.createStructField("idx", DataTypes.IntegerType, true);
    StructField f2 = DataTypes.createStructField("name", DataTypes.StringType, true);
    StructType schema1 = DataTypes.createStructType(Arrays.asList(f1, f2));

    Row r1 = RowFactory.create(3, "z");
    Row r2 = RowFactory.create(10, "a");
    Row r3 = RowFactory.create(5, "c");

    Dataset<Row> df = spark.createDataFrame(Arrays.asList(r1, r2, r3), schema1);
    df.orderBy("name", "idx").show();
    df.orderBy("idx", "name").show();
  }

  // 5.5.2.4.37절
  public static void runRollup(SparkSession spark, Dataset<Row> df) {
    Map<String, String> fnMap = new HashMap<>();
    fnMap.put("price", "sum");
    df.rollup("store", "product").agg(fnMap).show();
  }

  // 5.5.2.4.38절
  public static void runStat(SparkSession spark) {

    StructField f1 = DataTypes.createStructField("word", DataTypes.StringType, true);
    StructField f2 = DataTypes.createStructField("count", DataTypes.IntegerType, true);
    StructType schema1 = DataTypes.createStructType(Arrays.asList(f1, f2));

    Row r1 = RowFactory.create("a", 6);
    Row r2 = RowFactory.create("b", 4);
    Row r3 = RowFactory.create("c", 12);
    Row r4 = RowFactory.create("d", 6);

    Dataset<Row> df = spark.createDataFrame(Arrays.asList(r1, r2, r3, r4), schema1);
    df.show();
    df.stat().crosstab("word", "count").show();
  }

  // 5.5.2.4.39절
  public static void runWithColumn(SparkSession spark) {

    StructField f1 = DataTypes.createStructField("pname", DataTypes.StringType, true);
    StructField f2 = DataTypes.createStructField("price", DataTypes.StringType, true);
    StructType schema1 = DataTypes.createStructType(Arrays.asList(f1, f2));

    Row r1 = RowFactory.create("prod1", "100");
    Row r2 = RowFactory.create("prod2", "200");

    Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(r1, r2), schema1);
    Dataset<Row> df2 = df1.withColumn("dcprice", df1.col("price").multiply(0.9));
    Dataset<Row> df3 = df2.withColumnRenamed("dcprice", "newprice");

    df1.show();
    df2.show();
    df3.show();
  }

  // 5.5.2.4.40절
  public static void runSave(SparkSession spark) {
    String sparkHomeDir = "/Users/beginspark/Apps/spark";
    Dataset<Row> df = spark.read().json(sparkHomeDir + "/examples/src/main/resources/people.json");
    df.write().save("/Users/beginspark/Temp/default/" + System.currentTimeMillis());
    df.write().format("json").save("/Users/beginspark/Temp/json/" + System.currentTimeMillis());
    df.write().format("json").partitionBy("age").save("/Users/beginspark/Temp/parti/" + System.currentTimeMillis());
    df.write().mode(SaveMode.Overwrite).saveAsTable("ohMyTable");
    spark.sql("select * from ohMyTable").show();
    // bucketBy의 경우 테이블로 저장해야 햠
    df.write().format("json").bucketBy(20, "age").mode(SaveMode.Overwrite).saveAsTable("ohMyBuckedTable");
    spark.sql("select * from ohMyBuckedTable").show();
  }
}
