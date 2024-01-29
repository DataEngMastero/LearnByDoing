from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

def parseLine(x):
    x_split = x.split(",")
    age = int(x_split[2])
    no_of_friends = int(x_split[3])
    return (age, no_of_friends)


lines = sc.textFile("csvs/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]) )
averageByAge = totalsByAge.mapValues(lambda x: round(x[0]/x[1]) )
results = averageByAge.collect()

for result in results:
    print(result)