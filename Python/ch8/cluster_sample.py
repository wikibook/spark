from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.ml.clustering import KMeans
from pyspark.sql import functions

spark = SparkSession \
    .builder \
    .appName("cluster_sample") \
    .master("local[*]") \
    .getOrCreate()

# 원본데이터
# http://data.seoul.go.kr/openinf/sheetview.jsp?infId=OA-13061&tMenu=11
# 서울시 공공와이파이 위치정보 (영어)

d1 = spark.read.option("header", "true") \
    .option("sep", ",") \
    .option("inferSchema", True) \
    .option("mode", "DROPMALFORMED") \
    .csv("file:///Users/beginspark/Temp/data3.csv")

d1.printSchema()

d2 = d1.toDF("number", "name", "SI", "GOO", "DONG", "x", "y", "b_code", "h_code", "utmk_x", "utmk_y", "wtm_x", "wtm_y")

d3 = d2.select(d2.GOO.alias("loc"), d2.x, d2.y)

d3.show(5, False)

indexer = StringIndexer(inputCol="loc", outputCol="loccode")

assembler = VectorAssembler(inputCols=["loccode", "x", "y"], outputCol="features")

kmeans = KMeans(k=5, seed=1, featuresCol="features")

pipeline = Pipeline(stages=[indexer, assembler, kmeans])

model = pipeline.fit(d3)

d4 = model.transform(d3)

d4.groupBy("prediction") \
    .agg(functions.collect_set("loc").alias("loc")) \
    .orderBy("prediction").show(100, False)

WSSSE = model.stages[2].computeCost(d4)
print("Within Set Sum of Squared Errors = %d" % WSSSE)

print("Cluster Centers: ")
for v in model.stages[2].clusterCenters():
    print(v)

spark.stop
