from pyspark import SparkContext, SparkConf
import pyspark.pandas as ps

conf = SparkConf()
conf.setAppName("nyctaxi")
conf.set("spark_executor.memory", "20g")
conf.set("spark.executor.cores", "1")
conf.set("spark.driver.memory","20g")
conf.set("spark.driver.cores", "30")
spark = SparkContext(conf=conf)

df = ps.read_parquet("data/pickup_month_year")
df["count"] = 1

xy = df[["month", "count"]].groupby(["month"]).count().sort_index()
