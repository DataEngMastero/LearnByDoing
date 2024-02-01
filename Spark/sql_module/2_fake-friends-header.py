from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkSQL').getOrCreate()
people = spark.read.option("header", "true")   \
    .option("inferSchema", "true")  \
    .csv("../csvs/fakefriends-header.csv")

print("Here's the Schema : ")
people.printSchema()

print("Let's display the name column : ")
people.select("name").show()

print("Filter out anyone under 21 : ")
people.filter(people.age < 21).show()

print("Group BY Age")
people.groupBy("age").count().show()

print("Make everyone 10 yrs older")
people.select(people.name, people.age + 10).show()

spark.stop()