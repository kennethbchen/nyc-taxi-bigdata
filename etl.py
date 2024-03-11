from pyspark import SparkContext, SparkConf
import pyspark.pandas as ps

conf = SparkConf()
conf.setAppName("nyctaxi")
conf.set("spark_executor.memory", "20g")
conf.set("spark.executor.cores", "1")
conf.set("spark.driver.memory","20g")
conf.set("spark.driver.cores", "30")
spark = SparkContext(conf=conf)


df = ps.read_parquet("/data/nyctaxi/set1/part-r-007*.parquet")

# get only month and year each pickup happened on
df["month"] = df["pickup_datetime"].dt.month
df["year"] = df["pickup_datetime"].dt.year

df[["month", "year"]].to_parquet("data/pickup_month_year", mode="overwrite")
