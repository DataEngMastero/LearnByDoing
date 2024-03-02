from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_replace, col

# Create a SparkSession
spark = SparkSession.builder.appName("DynamicDelimiterText").getOrCreate()

# Define the file path
file_path = "users.txt"

# Read the text file into a DataFrame
df = spark.read.text(file_path)

# Define a regular expression pattern to match all non-alphanumeric characters except a comma
# This will help identify and replace delimiters with a comma
delimiter_pattern = "[^a-zA-Z0-9,]+"

# Replace any non-comma delimiter with a comma
df = df.withColumn("value", regexp_replace(col("value"), delimiter_pattern, ","))

# Split each line using a comma as the delimiter
# df = df.select(split(col("value"), ",").alias("fields"))

# Select the individual columns from the split result
# df = df.selectExpr("fields[0] as first_name", "fields[1] as last_name", "cast(fields[2] as int) as age")

# Show the DataFrame
df.show()

spark.stop()