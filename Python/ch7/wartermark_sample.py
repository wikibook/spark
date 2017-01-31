from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 7.4.3절
spark = SparkSession\
          .builder\
          .appName("WatermarkSample")\
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
    .toDF("words", "timestamp")

words = lines.select(explode(split(lines["words"], " ")).alias("word"), lines["timestamp"])

# 빠른 결과 확인을 위해서 워터마크 및 윈도우 주기를 초단위로 설정하였음 (실제로는 데이터 처리 기준에 맞춰 분 또는 시간 단위로 설정)
wordCount = words.withWatermark("timestamp", "5 seconds")\
                .groupBy(window(words["timestamp"], "10 seconds", "5 seconds"), words["word"])\
                .count()

# 완료모드 사용시 오래된 집계결과를 지우지 않음
query = wordCount.writeStream\
    .outputMode("complete")\
    .format("console")\
    .option("truncate", False)\
    .start()

query.awaitTermination()