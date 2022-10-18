from pyspark import SparkConf
from pyspark import SparkContext

def get_spark_context():
    return SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))