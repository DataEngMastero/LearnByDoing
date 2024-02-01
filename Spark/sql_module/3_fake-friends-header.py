from pyspark.sql import SparkSession, functions 

spark = SparkSession.builder.appName('SparkSQL').getOrCreate()
people = spark.read.option("header", "true")   \
    .option("inferSchema", "true")  \
    .csv("../csvs/fakefriends-header.csv")

print("Here's the Schema : ")
people.printSchema()

print("Group By Age")
friendsByAge = people.select("age", "friends")
# friendsByAge.groupBy("age").avg("friends").sort("age").show()

friendsByAge.groupBy("age") \
    .agg(functions.round(functions.avg("friends"),2).alias("friends_avg"))   \
    .sort("age").show()



spark.stop()