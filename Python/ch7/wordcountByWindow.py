from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 7.4.2절
spark = SparkSession\
          .builder\
          .appName("wordcount")\
          .master("local[*]")\
          .getOrCreate()

# port번호는 ncat 서버와 동일하게 수정해야 함
lines = spark\
    .readStream\
    .format("socket")\
    .option("host", "localhost")\
    .option("port", 9000)\
    .option("includeTimestamp", True)\
    .load()\
    .toDF("words", "ts")

words = lines.select(explode(split(col("words"), " ")).alias("word"), window(col("ts"), "10 minute", "5 minute").alias("time"));
wordCount = words.groupBy("time", "word").count()

query = wordCount.writeStream\
      .outputMode("complete")\
      .option("truncate", False)\
      .format("console")\
      .start();

query.awaitTermination()    

 
