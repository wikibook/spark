import collections

from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

from word import Word

spark = SparkSession \
    .builder \
    .appName("sample") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "file:///Users/beginspark/Temp/") \
    .conf.set("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

sc = spark.sparkContext

# 파이썬에서 데이터프레임 생성 시 네임드튜플(namedtuple), 튜플(tuple)
# Row, 커스텀 클래스(class), 딕셔너리(dictionary) 등을
# 사용하여 생성할 수 있다
Person = collections.namedtuple('Person', 'name age job')

# sample dataframe 1
row1 = Person(name="hayoon", age=7, job="student")
row2 = Person(name="sunwoo", age=13, job="student")
row3 = Person(name="hajoo", age=5, job="kindergartener")
row4 = Person(name="jinwoo", age=13, job="student")
data = [row1, row2, row3, row4]
sample_df = spark.createDataFrame(data)

d1 = ("store2", "note", 20, 2000)
d2 = ("store2", "bag", 10, 5000)
d3 = ("store1", "note", 15, 1000)
d4 = ("store1", "pen", 20, 5000)
sample_df2 = spark.createDataFrame([d1, d2, d3, d4]).toDF("store", "product", "amount", "price")

ldf = spark.createDataFrame([Word("w1", 1), Word("w2", 1)])
rdf = spark.createDataFrame([Word("w1", 1), Word("w3", 1)])


# createDataFrame
def createDataFrame(spark, sc):
    sparkHomeDir = "file:/Users/beginspark/Apps/spark"

    # 1. 파일로 부터 생성
    df1 = spark.read.json(sparkHomeDir + "/examples/src/main/resources/people.json")
    df2 = spark.read.parquet(sparkHomeDir + "/examples/src/main/resources/users.parquet")
    df3 = spark.read.text(sparkHomeDir + "/examples/src/main/resources/people.txt")

    # 2. 컬렉션으로부터 생성 (ex5-17)
    row1 = Row(name="hayoon", age=7, job="student")
    row2 = Row(name="sunwoo", age=13, job="student")
    row3 = Row(name="hajoo", age=5, job="kindergartener")
    row4 = Row(name="jinwoo", age=13, job="student")
    data = [row1, row2, row3, row4]
    df4 = spark.createDataFrame(data)

    # 3. RDD로부터 생성 (ex5-20)
    rdd = spark.sparkContext.parallelize(data)
    df5 = spark.createDataFrame(data)

    # 4. 스키마 지정(ex5-23)
    sf1 = StructField("name", StringType(), True)
    sf2 = StructField("age", IntegerType(), True)
    sf3 = StructField("job", StringType(), True)
    schema = StructType([sf1, sf2, sf3])
    r1 = ("hayoon", 7, "student")
    r2 = ("sunwoo", 13, "student")
    r3 = ("hajoo", 5, "kindergartener")
    r4 = ("jinwoo", 13, "student")
    rows = [r1, r2, r3, r4]
    df6 = spark.createDataFrame(rows, schema)


# 5.5.2.1.1절 ~ 5.5.2.2.4절
def runBasicOpsEx(spark, sc, df):
    df.show()
    df.head()
    df.first()
    df.take(2)
    df.count()
    df.collect()
    df.describe("age").show()
    df.persist(StorageLevel.MEMORY_AND_DISK_2)
    df.printSchema()
    df.columns
    df.dtypes
    df.schema
    df.createOrReplaceTempView("users")
    spark.sql("select name, age from users where age > 20").show()
    spark.sql("select name, age from users where age > 20").explain()


# 5.5.2.4절
def runColumnEx(spark, sc, df):
    df.where(df.age > 10).show()


# 5.5.2.4.2절
def runAlias(spark, sc, df):
    df.select(df.age + 1).show()
    df.select((df.age + 1).alias("age")).show()


# 5.5.2.4.3절
def runIsinEx(spark, sc):
    nums = spark.sparkContext.broadcast([1, 3, 5, 7, 9])
    rdd = spark.sparkContext.parallelize(range(0, 10)).map(lambda v: Row(v))
    df = spark.createDataFrame(rdd)
    df.where(df._1.isin(nums.value)).show()


# 5.5.2.4.4절
def runWhenEx(spark, sc):
    ds = spark.range(0, 5)
    col = when(ds.id % 2 == 0, "even").otherwise("odd").alias("type")
    ds.select(ds.id, col).show()


# 5.5.2.4.5절
def runMaxMin(spark, df):
    min_col = min("age")
    max_col = max("age")
    df.select(min_col, max_col).show()


# 5.5.2.4.6절 ~ 5.5.2.4.9절
def runAggregateFunctions(spark, df1, df2):
    # collect_list, collect_set
    doubledDf1 = df1.union(df1)
    doubledDf1.select(functions.collect_list(doubledDf1["name"])).show(truncate=False)
    doubledDf1.select(functions.collect_set(doubledDf1["name"])).show(truncate=False)

    # count, countDistinct
    doubledDf1.select(functions.count(doubledDf1["name"]), functions.countDistinct(doubledDf1["name"])).show(
        truncate=False)

    # sum
    df2.printSchema()
    df2.select(sum(df2["price"])).show(truncate=False)

    # grouping, grouping_id
    df2.cube(df2["store"], df2["product"]).agg(sum(df2["amount"]), grouping(df2["store"])).show(truncate=False)
    df2.cube(df2["store"], df2["product"]).agg(sum(df2["amount"]), grouping_id(df2["store"], df2["product"])).show(
        truncate=False)


# 5.5.2.4.10 ~ 5.5.2.4.11 절
def runCollectionFunctions(spark):
    df = spark.createDataFrame([{'numbers': '9,1,5,3,9'}])
    arrayCol = split(df.numbers, ",")

    # array_contains, size
    df.select(arrayCol, array_contains(arrayCol, 2), size(arrayCol)).show(truncate=False)

    # sort_array()
    df.select(arrayCol, sort_array(arrayCol)).show(truncate=False)

    # explode, posexplode
    df.select(explode(arrayCol)).show(truncate=False)
    df.select(posexplode(arrayCol)).show(truncate=False)


# 5.5.2.4.10 ~ 5.5.2.4.11 절
def runCollectionFunctions(spark):
    df = spark.createDataFrame([{'numbers': '9,1,5,3,9'}])
    arrayCol = split(df.numbers, ",")

    # array_contains, size
    df.select(arrayCol, array_contains(arrayCol, 2), size(arrayCol)).show(truncate=False)

    # sort_array()
    df.select(arrayCol, sort_array(arrayCol)).show(truncate=False)

    # explode, posexplode
    df.select(explode(arrayCol)).show(truncate=False)
    df.select(posexplode(arrayCol)).show(truncate=False)


# 5.5.2.4.12 ~ 5.5.2.4.14절
def runDateFunctions(spark):
    f1 = StructField("d1", StringType(), True)
    f2 = StructField("d2", StringType(), True)
    schema1 = StructType([f1, f2])

    df = spark.createDataFrame([("2017-12-25 12:00:05", "2017-12-25")], schema1)
    df.show(truncate=False)

    # current_date, unix_timestamp, to_date
    d3 = current_date().alias("d3")
    d4 = unix_timestamp(df["d1"].alias("d4"))
    d5 = to_date(df["d2"].alias("d5"))
    d6 = to_date(d4.cast("timestamp")).alias("d6")
    df.select(df["d1"], df["d2"], d3, d4, d5, d6).show(truncate=False)

    # add_months, date_add, last_day
    d7 = add_months(d6, 2).alias("d7")
    d8 = date_add(d6, 2).alias("d8")
    d9 = last_day(d6).alias("d9")
    df.select(df["d1"], df["d2"], d7, d8, d9).show(truncate=False)

    # window
    f3 = StructField("date", StringType(), True)
    f4 = StructField("product", StringType(), True)
    f5 = StructField("amount", IntegerType(), True)
    schema2 = StructType([f3, f4, f5])

    r2 = ("2017-12-25 12:01:00", "note", 1000)
    r3 = ("2017-12-25 12:01:10", "pencil", 3500)
    r4 = ("2017-12-25 12:03:20", "pencil", 23000)
    r5 = ("2017-12-25 12:05:00", "note", 1500)
    r6 = ("2017-12-25 12:05:07", "note", 2000)
    r7 = ("2017-12-25 12:06:25", "note", 1000)
    r8 = ("2017-12-25 12:08:00", "pencil", 500)
    r9 = ("2017-12-25 12:09:45", "note", 30000)

    dd = spark.createDataFrame([r2, r3, r4, r5, r6, r7, r8, r9], schema2);

    timeCol = unix_timestamp(dd["date"]).cast("timestamp");
    windowCol = window(timeCol, "5 minutes");
    dd.groupBy(windowCol, dd["product"]).agg(sum(dd["amount"])).show(truncate=False);


# 5.5.2.4.15절
def runDateFunctions(spark):
    # 파이썬의 경우 아래와 같이 튜플을 이용하여 데이터프레임을 생성하는 것도 가능함
    df1 = spark.createDataFrame([(1.512,), (2.234,), (3.42,)], ['value'])
    df2 = spark.createDataFrame([(25.0,), (9.0,), (10.0,)], ['value'])

    df1.select(round(df1["value"], 1)).show()
    df2.select(functions.sqrt('value')).show()


# 5.5.2.4.16 ~ 5.5.2.4.20절
def runOtherFunctions(spark, personDf):
    df = spark.createDataFrame([("v1", "v2", "v3")], ["c1", "c2", "c3"]);

    # array
    df.select(df.c1, df.c2, df.c3, array("c1", "c2", "c3").alias("newCol")).show(truncate=False)

    # desc, asc
    personDf.show()
    personDf.sort(functions.desc("age"), functions.asc("name")).show()

    # pyspark 2.1.0 버전은 desc_nulls_first, desc_nulls_last, asc_nulls_first, asc_nulls_last 지원하지 않음

    # split, length (pyspark에서 컬럼은 df["col"] 또는 df.col 형태로 사용 가능)
    df2 = spark.createDataFrame([("Splits str around pattern",)], ['value'])
    df2.select(df2.value, split(df2.value, " "), length(df2.value)).show(truncate=False)

    # rownum, rank
    f1 = StructField("date", StringType(), True)
    f2 = StructField("product", StringType(), True)
    f3 = StructField("amount", IntegerType(), True)
    schema = StructType([f1, f2, f3])

    p1 = ("2017-12-25 12:01:00", "note", 1000)
    p2 = ("2017-12-25 12:01:10", "pencil", 3500)
    p3 = ("2017-12-25 12:03:20", "pencil", 23000)
    p4 = ("2017-12-25 12:05:00", "note", 1500)
    p5 = ("2017-12-25 12:05:07", "note", 2000)
    p6 = ("2017-12-25 12:06:25", "note", 1000)
    p7 = ("2017-12-25 12:08:00", "pencil", 500)
    p8 = ("2017-12-25 12:09:45", "note", 30000)

    dd = spark.createDataFrame([p1, p2, p3, p4, p5, p6, p7, p8], schema)
    w1 = Window.partitionBy("product").orderBy("amount")
    w2 = Window.orderBy("amount")
    dd.select(dd.product, dd.amount, functions.row_number().over(w1).alias("rownum"),
              functions.rank().over(w2).alias("rank")).show()


# 5.5.2.4.21절
def runUDF(spark, df):
    # functions를 이용한 등록
    fn1 = functions.udf(lambda job: job == "student")
    df.select(df["name"], df["age"], df["job"], fn1(df["job"])).show()
    # SparkSession을 이용한 등록
    spark.udf.register("fn2", lambda job: job == "student")
    df.createOrReplaceTempView("persons")
    spark.sql("select name, age, job, fn2(job) from persons").show()


# 5.5.2.4.24절
def runAgg(spark, df):
    df.agg(max("amount"), min("price")).show()
    df.agg({"amount": "max", "price": "min"}).show()


# 5.5.2.4.26절
def runDfAlias(spark, df):
    df.select(df["product"]).show()
    df.alias("aa").select("aa.product").show()


# 5.5.2.4.27절
def runGroupBy(spark, df):
    df.groupBy("store", "product").agg({"price": "sum"}).show()


# 5.5.3.4.28절
def runCube(spark, df):
    df.cube("store", "product").agg({"price": "sum"}).show()


# 5.5.2.4.29절
def runDistinct(spark):
    d1 = ("store1", "note", 20, 2000)
    d2 = ("store1", "bag", 10, 5000)
    d3 = ("store1", "note", 20, 2000)
    rows = [d1, d2, d3]
    cols = ["store", "product", "amount", "price"]
    df = spark.createDataFrame(rows, cols)
    df.distinct().show()
    df.dropDuplicates(["store"]).show()


# 5.5.2.4.30절
def runDrop(spark, df):
    df.drop(df["store"]).show()


# 5.5.2.4.31절
def runIntersect(spark):
    a = spark.range(1, 5)
    b = spark.range(2, 6)
    c = a.intersect(b)
    c.show()


# 5.5.2.4.32절
def runExcept(spark):
    df1 = spark.range(1, 6)
    df2 = spark.createDataFrame([(2,), (4,)], ['value'])
    # 파이썬의 경우 except 대신 subtract 메서드 사용
    # subtract의 동작은 except와 같음
    df1.subtract(df2).show()


# 5.5.2.4.33절
def runJoin(spark, ldf, rdf):
    joinTypes = "inner,outer,leftouter,rightouter,leftsemi".split(",")
    for joinType in joinTypes:
        print("============= %s ===============" % joinType)
        ldf.join(rdf, ["word"], joinType).show()


# 5.5.2.4.35절
def runNa(spark, ldf, rdf):
    result = ldf.join(rdf, ["word"], "outer").toDF("word", "c1", "c2")
    result.show()
    # 파이썬의 경우 na.drop또는 dropna 사용 가능
    # c1과 c2 칼럼의 null이 아닌 값의 개수가 thresh 이하일 경우 drop
    # thresh=1로 설정할 경우 c1 또는 c2 둘 중의 하나만 null 아닌 값을 가질 경우
    # 결과에 포함시킨다는 의미가 됨
    result.na.drop(thresh=2, subset=["c1", "c2"]).show()
    result.dropna(thresh=2, subset=["c1", "c2"]).show()
    # fill
    result.na.fill({"c1": 0}).show()
    # 파이썬의 경우 to_replace에 딕셔너리를 지정하여 replace를 수행(이 경우 value에 선언한 값은 무시됨
    # 딕셔너리를 사용하지 않을 경우 키 목록(첫번째 인자)과 값 목록(두번째 인자)을 지정하여 replace 수행
    result.na.replace(to_replace={"w1": "word1", "w2": "word2"}, value="", subset="word").show()
    result.na.replace(["w1", "w2"], ["word1", "word2"], "word").show()


# 5.5.2.4.36절
def runOrderBy(spark):
    df = spark.createDataFrame([(3, "z"), (10, "a"), (5, "c")], ["idx", "name"])
    df.orderBy("name", "idx").show()
    df.orderBy("idx", "name").show()


# 5.5.2.4.37절
def runRollup(spark, df):
    df.rollup("store", "product").agg({"price": "sum"}).show();


# 5.5.2.4.38절
def runStat(spark):
    df = spark.createDataFrame([("a", 6), ("b", 4), ("c", 12), ("d", 6)], ["word", "count"])
    df.show()
    df.stat.crosstab("word", "count").show()


# 5.5.2.4.39절
def runWithColumn(spark):
    df1 = spark.createDataFrame([("prod1", "100"), ("prod2", "200")], ["pname", "price"])
    df2 = df1.withColumn("dcprice", df1["price"] * 0.9)
    df3 = df2.withColumnRenamed("dcprice", "newprice")
    df1.show()
    df2.show()
    df3.show()


# 5.5.2.4.40절
def runSave(spark):
    sparkHomeDir = "file:///Users/beginspark/Apps/spark"
    df = spark.read.json(sparkHomeDir + "/examples/src/main/resources/people.json")
    df.write.save("/Users/beginspark/Temp/default/%d" % time.time())
    df.write.format("json").save("/Users/beginspark/Temp/json/%d" % time.time())
    df.write.format("json").partitionBy("age").save("/Users/beginspark/Temp/parti/%d" % time.time())
    # saveMode: append, overwrite, error, ignore
    df.write.mode("overwrite").saveAsTable("ohMyTable")
    spark.sql("select * from ohMyTable").show()
    # 파이썬의 경우 bucketBy 지원하지 않음

# [예제 실행 방법] 아래에서 원하는 예제의 주석을 제거하고 실행!!

# runBasicOpsEx(spark, sc, sample_df)
# createDataFrame(spark, sc)
# runColumnEx(spark, sc, sample_df)
# runAlias(spark, sc, sample_df)
# runIsinEx(spark, sc)
# runWhenEx(spark, sc)
# runMaxMin(spark, sample_df)
# runAggregateFunctions(spark, sample_df, sample_df2)
# runCollectionFunctions(spark)
# runDateFunctions(spark)
# runDateFunctions(spark)
# runOtherFunctions(spark, sample_df)
# runUDF(spark, sample_df)
# runAgg(spark, sample_df2)
# runDfAlias(spark, sample_df2)
# runGroupBy(spark, sample_df2)
# runCube(spark, sample_df2)
# runDistinct(spark)
# runDrop(spark, sample_df2)
# runIntersect(spark)
# runExcept(spark)
# runJoin(spark, ldf, rdf)
# runNa(spark, ldf, rdf)
# runOrderBy(spark)
# runRollup(spark, sample_df2)
# runWithColumn(spark)
# runSave(spark)
