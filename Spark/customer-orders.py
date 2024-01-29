from pyspark import SparkConf, SparkContext 
import collections

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf=conf)

def parseLines(line):
    fields = line.split(",")
    customer_id = int(fields[0])
    price = float(fields[2])
    return (customer_id, price)


lines = sc.textFile("csvs/customer-orders.csv")
rdd = lines.map(parseLines)
customerPrice = rdd.reduceByKey(lambda x, y: x + y)
customerPriceSorted = customerPrice.map(lambda x: (x[1], x[0])).sortByKey()
results = customerPriceSorted.collect()

for result in results: 
    print(f"{result[1]} : {round(result[0], 2)}")