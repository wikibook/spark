import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
from pyspark.sql import SparkSession

spark = SparkSession\
          .builder\
          .appName("vector_sample")\
          .master("local[*]")\
          .getOrCreate()
          
sc = spark.sparkContext

# 8.1.1절
# dense vector
v1 = np.array([0.1, 0.0, 0.2, 0.3])
v2 = Vectors.dense([0.1, 0.0, 0.2, 0.3])
v3 = [0.1, 0.0, 0.2, 0.3]

# sparse vector
v3 = Vectors.sparse(4, [(0, 0.1), (2, 0.2), (3, 0.3)])
v4 = Vectors.sparse(4, [0, 2, 3], [0.1, 0.2, 0.3])
v5 = sps.csc_matrix((np.array([0.1, 0.2, 0.3]), np.array([0, 2, 3]), np.array([0, 3])), shape = (4, 1))

print(v1)
print(v3.toArray())
print(v5)

# 8.1.2절
v6 = LabeledPoint(1.0, v1)
print("label:%s, features:%s" % (v6.label, v6.features))

path = "file:///Users/beginspark/Apps/spark/data/mllib/sample_libsvm_data.txt"
v7 = MLUtils.loadLibSVMFile(spark.sparkContext, path)
lp1 = v7.first()
print("label:%s, features:%s" % (lp1.label, lp1.features))


spark.stop
