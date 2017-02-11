from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.pipeline import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql import functions

spark = SparkSession \
    .builder \
    .appName("classfication_sample") \
    .master("local[*]") \
    .getOrCreate()


def labelFn(avr_month, avr_total):
    if (avr_month - avr_total) >= 0:
        return 1.0
    else:
        return 0.0


# Label(혼잡:1.0, 원활:0.0)
label = functions.udf(lambda avr_month, avr_total: labelFn(avr_month, avr_total))

# 원본데이터
# http://data.seoul.go.kr/openinf/sheetview.jsp?infId=OA-2604
# 서울시 도시고속도로 월간 소통 통계

d1 = spark.read.option("header", "true") \
    .option("sep", ",").option("inferSchema", True) \
    .option("mode", "DROPMALFORMED") \
    .csv("file:///Users/beginspark/Temp/data2.csv")

d2 = d1.toDF("year", "month", "road", "avr_traffic_month", "avr_velo_month", "mon", "tue", "wed", "thu", "fri", "sat",
             "sun")

# data 확인
d2.printSchema()

# null 값 제거
d3 = d2.where("avr_velo_month is not null")

# 도로별 평균 속도
d4 = d3.groupBy("road").agg(functions.round(functions.avg("avr_velo_month"), 1).alias("avr_velo_total"))
d5 = d3.join(d4, ["road"])

# label 부여
d6 = d5.withColumn("label", label(d5.avr_velo_month, d5.avr_velo_total).cast("double"))
d6.select("road", "avr_velo_month", "avr_velo_total", "label").show(5, False)
d6.groupBy("label").count().show(truncate=False)

dataArr = d6.randomSplit([0.7, 0.3])
train = dataArr[0]
test = dataArr[1]

indexer = StringIndexer(inputCol="road", outputCol="roadcode")

assembler = VectorAssembler(inputCols=["roadcode", "mon", "tue", "wed", "thu", "fri", "sat", "sun"],
                            outputCol="features")

dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")

pipeline = Pipeline(stages=[indexer, assembler, dt])

model = pipeline.fit(train)

predict = model.transform(test)

predict.select("label", "probability", "prediction").show(3, False)

# areaUnderROC, areaUnderPR
evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

print(evaluator.evaluate(predict))

treeModel = model.stages[2]
print("Learned classification tree model:%s" % treeModel.toDebugString)

spark.stop
