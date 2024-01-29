from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("MinTemperature")
sc = SparkContext(conf=conf)

def parseLine(x):
    fields = x.split(",")
    station_id = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (station_id, entry_type, temperature)

lines = sc.textFile("csvs/1800.csv")
rdd = lines.map(parseLine)
minTemps = rdd.filter(lambda x: "TMAX" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: max(x,y))
results = stationTemps.collect()

for result in results:
    print(result[0] + "\t{:.3}F".format(result[1]))