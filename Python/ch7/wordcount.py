from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 7.2절
spark = SparkSession \
    .builder \
    .appName("wordcount") \
    .master("local[*]") \
    .getOrCreate()

# port번호는 ncat 서버와 동일하게 수정해야 함
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9000) \
    .load()

words = lines.select(explode(split(col("value"), " ")).alias("word"))
wordCount = words.groupBy("word").count()
query = wordCount.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start();

query.awaitTermination()
