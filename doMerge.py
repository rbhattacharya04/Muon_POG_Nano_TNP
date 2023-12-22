from __future__ import print_function
import os
import sys
import glob
import itertools

from pyspark.ml.feature import Bucketizer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType

from pyspark.sql.functions import col
from pyspark.sql.types import LongType

import subprocess

spark = SparkSession\
    .builder\
    .master("yarn")\
    .appName("TnP")

spark = spark\
    .config("spark.sql.broadcastTimeout", "36000")\
    .config("spark.network.timeout", "600s")\
    .config("spark.driver.memory", "6g")\
    .config("spark.executor.memory", "10g")\
    .config("spark.executorEnv.PYTHONPATH", os.environ.get('PYTHONPATH'))\
    .config("spark.executorEnv.LD_LIBRARY_PATH", os.environ.get('LD_LIBRARY_PATH'))

spark = spark.getOrCreate()

n = len(sys.argv)
print(sys.argv)
if n<3:
    print("Error: Not enough input parameters \n")
    print("Try like this: \n")
    print("doMerge.py inputDir outputFile")
    print("------------------------------")
    sys.exit()

input_directory = sys.argv[1]
outFile = sys.argv[2]
if n==4:
    batch = int(sys.argv[3])
else:
    batch = 100

print("Start running doMerge.py!")
print(">>>>> Input directory : " + input_directory)
print(">>>>> Output file     : " + outFile)
print(">>>>> batch           : " + str(batch))
print("------------------------------")

if ("hdfs://analytix" not in input_directory) or ("hdfs://analytix" not in outFile):
    print(">>>>>>>> Remember to use the correct Hadoop syntax")
    print(">>>>>>>> Path should start with:  hdfs://analytix")
    print("--------------------------------------------------")
    sys.exit()

cmd = "hdfs dfs -find {} -name '*.parquet'".format(input_directory)
fnames = subprocess.check_output(cmd, shell=True).strip().split(b'\n')
fnames = [fname.decode('ascii') for fname in fnames]

print(fnames[0:10])

first = True

while fnames:
    current = fnames[:batch]
    fnames = fnames[batch:]

    baseDF = spark.read.option("mergeSchema","false").parquet(*current)
    if first:
        baseDF = spark.read.parquet(*current)
        schema = baseDF.schema # Force the same schema in the next iterations
    else:
        baseDF = spark.read.schema(schema).parquet(*current)

    if first:
        baseDF.write.parquet(outFile)
        first = False
    else:
        baseDF.write.mode('append').parquet(outFile)





