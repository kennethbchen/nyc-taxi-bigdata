from pyspark import SparkContext, SparkConf
import pyspark.pandas as ps

import matplotlib.pyplot as plt

conf = SparkConf()
conf.setAppName("nyctaxi")
conf.set("spark_executor.memory", "20g")
conf.set("spark.executor.cores", "1")
conf.set("spark.driver.memory","20g")
conf.set("spark.driver.cores", "30")
spark = SparkContext(conf=conf)

df = ps.read_parquet("data/pickup_month_year")
df["count"] = 1

x = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
y = df[["month", "count"]].groupby(["month"]).count().sort_index().values.flatten()


plt.bar(x, y)

plt.title("Total Trips Per Month")
plt.xlabel("Month")
plt.ylabel("Trips")

plt.xticks(rotation=45)

plt.tight_layout()
plt.savefig("images/1_trips_per_month")

plt.clf()
