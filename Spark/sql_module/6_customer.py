from pyspark.sql import SparkSession, functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("CustomerOrders").getOrCreate()

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("amount", FloatType(), True)
])

df = spark.read.schema(schema).csv("../csvs/customer-orders.csv")
df.printSchema()

customerRecords = df.select("customer_id", "amount")

amountByCustomers = customerRecords.groupBy("customer_id").agg(func.round(func.sum("amount"), 2).alias("amount_spent"))
amountByCustomersSorted = amountByCustomers.sort("amount_spent")

amountByCustomersSorted.show(amountByCustomersSorted.count())

spark.stop()