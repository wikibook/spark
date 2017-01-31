from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

# 7.3절
spark = SparkSession\
          .builder\
          .appName("structuredsample")\
          .master("local[*]")\
          .getOrCreate()

# 예제를 실행 후
# "<spark_home>/examples/src/main/resources/peple.json" 파일을
# json("..") 메서드에 지정한 디렉토리에 복사하여 결과 확인
def runReadStreamSample():

    source = spark\
        .readStream\
        .schema(StructType().add("name", "string").add("age", "integer"))\
        .json("file:///Users/beginspark/Temp/json")

    result = source.groupBy("name").count()

    result\
        .writeStream\
        .outputMode("complete")\
        .format("console")\
        .start()\
        .awaitTermination()

runReadStreamSample() 
