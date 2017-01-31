from pyspark.ml.feature import Tokenizer
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("tokenizer_sample") \
    .master("local[*]") \
    .getOrCreate()

data = [(0, "Tokenization is the process"), (1, "Refer to the Tokenizer")]
inputDF = spark.createDataFrame(data).toDF("id", "input")
tokenizer = Tokenizer(inputCol="input", outputCol="output")
outputDF = tokenizer.transform(inputDF)
outputDF.printSchema()
outputDF.show()

spark.stop
