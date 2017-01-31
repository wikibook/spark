from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("pipeline_sample") \
    .master("local[*]") \
    .getOrCreate()

# 훈련용 데이터 (키, 몸무게, 나이, 성별)
training = spark.createDataFrame([
    (161.0, 69.87, 29, 1.0),
    (176.78, 74.35, 34, 1.0),
    (159.23, 58.32, 29, 0.0)]).toDF("height", "weight", "age", "gender")

training.cache()

# 테스트용 데이터
test = spark.createDataFrame([
    (169.4, 75.3, 42),
    (185.1, 85.0, 37),
    (161.6, 61.2, 28)]).toDF("height", "weight", "age")

training.show(truncate=False)

assembler = VectorAssembler(inputCols=["height", "weight", "age"], outputCol="features")

# training 데이터에 features 컬럼 추가
assembled_training = assembler.transform(training)

assembled_training.show(truncate=False)

# 모델 생성 알고리즘 (로지스틱 회귀 평가자)
lr = LogisticRegression(maxIter=10, regParam=0.01, labelCol="gender")

# 모델 생성
model = lr.fit(assembled_training)

# 예측값 생성
model.transform(assembled_training).show()

# 파이프라인
pipeline = Pipeline(stages=[assembler, lr])

# 파이프라인 모델 생성
pipelineModel = pipeline.fit(training)

# 파이프라인 모델을 이용한 예측값 생성
pipelineModel.transform(training).show()

path1 = "/Users/beginspark/Temp/regression-model"
path2 = "/Users/beginspark/Temp/pipelinemodel"

# 모델 저장
model.write().overwrite().save(path1)
pipelineModel.write().overwrite().save(path2)

# 저장된 모델 불러오기
loadedModel = LogisticRegressionModel.load(path1)
loadedPipelineModel = PipelineModel.load(path2)

spark.stop
