import collections

from pyspark.sql import SparkSession

# 5.6절 하이브 연동
spark = SparkSession \
    .builder \
    .appName("sample") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "file:///Users/beginspark/Temp") \
    .enableHiveSupport() \
    .getOrCreate()

Person = collections.namedtuple('Person', 'name age job')

# sample dataframe 1
row1 = Person(name="hayoon", age=7, job="student")
row2 = Person(name="sunwoo", age=13, job="student")
row3 = Person(name="hajoo", age=5, job="kindergartener")
row4 = Person(name="jinwoo", age=13, job="student")
data = [row1, row2, row3, row4]
df = spark.createDataFrame(data)

spark.sql("CREATE TABLE IF NOT EXISTS Persons (name STRING, age INT, job STRING)")
spark.sql("show tables").show()

df.write.mode("overwrite").saveAsTable("Users")
spark.sql("show tables").show()

spark.stop