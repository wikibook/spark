from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql import functions

spark = SparkSession \
    .builder \
    .appName("regression_sample") \
    .master("local[*]") \
    .getOrCreate()

# 데이터 제공처 : 서울시 학생 체격 현황 (키, 몸무게) 통계
# (http://data.seoul.go.kr/openinf/linkview.jsp?infId=OA-12381&tMenu=11)
df1 = spark.read.option("header", "false") \
    .option("sep", "\t") \
    .option("inferSchema", True) \
    .csv("file:///Users/beginspark/Temp/Octagon.txt")

df1.printSchema()
df1.show(5, False)

# Header 제거
df2 = df1.where("_c0 != '기간' ")
df2.show(3, False)

strim = functions.udf(lambda v: float(v.replace("[^0-9.]", "")))

# cache
df2.cache()

# 초등학교 남 키, 몸무게
df3 = df2.select(df2._c0.alias("year"),
                 strim(df2._c2).cast("double").alias("height"),
                 strim(df2._c4).cast("double").alias("weight")) \
    .withColumn("grade", functions.lit("elementary")) \
    .withColumn("gender", functions.lit("man"))

# 초등학교 여 키, 몸무게
df4 = df2.select(df2._c0.alias("year"),
                 strim(df2._c3).cast("double").alias("height"),
                 strim(df2._c5).cast("double").alias("weight")) \
    .withColumn("grade", functions.lit("elementary")) \
    .withColumn("gender", functions.lit("woman"))

# 중학교 남 키, 몸무게
df5 = df2.select(df2._c0.alias("year"),
                 strim(df2._c6).cast("double").alias("height"),
                 strim(df2._c8).cast("double").alias("weight")) \
    .withColumn("grade", functions.lit("middle")) \
    .withColumn("gender", functions.lit("man"))

# 중학교 여 키, 몸무게
df6 = df2.select(df2._c0.alias("year"),
                 strim(df2._c7).cast("double").alias("height"),
                 strim(df2._c9).cast("double").alias("weight")) \
    .withColumn("grade", functions.lit("middle")) \
    .withColumn("gender", functions.lit("woman"))

# 고등학교 남 키, 몸무게
df7 = df2.select(df2._c0.alias("year"),
                 strim(df2._c10).cast("double").alias("height"), \
                 strim(df2._c12).cast("double").alias("weight")) \
    .withColumn("grade", functions.lit("high")) \
    .withColumn("gender", functions.lit("man"))

# 고등학교 여 키, 몸무게
df8 = df2.select(df2._c0.alias("year"),
                 strim(df2._c11).cast("double").alias("height"),
                 strim(df2._c13).cast("double").alias("weight")) \
    .withColumn("grade", functions.lit("high")) \
    .withColumn("gender", functions.lit("woman"))

df9 = df3.union(df4).union(df5).union(df6).union(df7).union(df8)

# 연도, 키, 몸무게, 학년, 성별
df9.show(5, False)
df9.printSchema()

# 문자열 컬럼을 double로 변환
gradeIndexer = StringIndexer(inputCol="grade", outputCol="gradecode")

genderIndexer = StringIndexer(inputCol="gender", outputCol="gendercode")

df10 = gradeIndexer.fit(df9).transform(df9)
df11 = genderIndexer.fit(df10).transform(df10)

df11.show(3, False)
df11.printSchema()

assembler = VectorAssembler(inputCols=["height", "gradecode", "gendercode"], outputCol="features")

df12 = assembler.transform(df11)

df12.show(truncate=False)

samples = df12.randomSplit([0.7, 0.3])
training = samples[0]
test = samples[1]

lr = LinearRegression(maxIter=5, regParam=0.3, labelCol="weight", featuresCol="features", predictionCol="predic_weight")

model = lr.fit(training)

print("결정계수(R2):%d" % model.summary.r2)

d13 = model.transform(test)
d13.cache()

d13.select("weight", "predic_weight").show(5, False)

evaluator = RegressionEvaluator(labelCol="weight", predictionCol="predic_weight")

# root mean squared error
rmse = evaluator.evaluate(d13)

# mean squared error
mse = evaluator.setMetricName("mse").evaluate(d13)

# R2 metric
r2 = evaluator.setMetricName("r2").evaluate(d13)

# mean absolute error
mae = evaluator.setMetricName("mae").evaluate(d13)

print("rmse:%d, mse:%d, r2:%d, mae:%d" % (rmse, mse, r2, mae))

# 파이프라인
pipeline = Pipeline(stages=[gradeIndexer, genderIndexer, assembler, lr])
samples2 = df9.randomSplit([0.7, 0.3])
training2 = samples2[0]
test2 = samples2[1]

# 파이프라인 모델 생성
pipelineModel = pipeline.fit(training2)

# 파이프라인 모델을 이용한 예측값 생성
pipelineModel.transform(test2).show(5, False)

spark.stop
