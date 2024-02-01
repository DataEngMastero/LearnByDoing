from pyspark.sql import SparkSession, functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

spark = SparkSession.builder.appName("MinTemp").getOrCreate()

schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("measure_type", StringType(), True),
    StructField("temperature", FloatType(), True)
])

df = spark.read.schema(schema).csv("../csvs/1800.csv")
df.printSchema()


minTemps = df.filter(df.measure_type == "TMIN")
stationTemps = minTemps.select("station_id", "temperature")
minTempsByStation = stationTemps.groupBy("station_id").min("temperature")
minTempsByStation.show()


minTempsByStationF = minTempsByStation \
    .withColumn("temperature",
        func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2)) \
        .select("station_id", "temperature").sort("temperature")

results = minTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.3}F".format(result[1]))

spark.stop()