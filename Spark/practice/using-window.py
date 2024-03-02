from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_date, sum, month,expr, date_format, from_unixtime
from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("CRevenue").getOrCreate()

df = spark.read.option("header", "true")   \
    .option("inferSchema", "true")  \
    .csv("revenue.csv")

# Convert the "date" column to a date type
df = df.withColumn("months", month(to_date(col("date"), "dd-MMM")))

# Create a window specification partitioned by month and ordered by date
window_spec = Window.partitionBy(df["months"]).orderBy(df["months"])

# # Calculate the cumulative revenue for each month
df = df.withColumn("cumulative_revenue", sum(col("revenue")).over(window_spec))
df = df.withColumn("month", expr('substr(date, 4,3)')).select("month", "cumulative_revenue").distinct()

# Create a new column "month_abbreviation" to store the month abbreviations
df.show()


# spark-submit --conf spark.log.level=WARN using-window.py