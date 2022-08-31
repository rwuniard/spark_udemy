from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    custId = int(fields[0])
    amountDollar = float(fields[2])
    return (custId, amountDollar)

lines = sc.textFile("file:///Users/rwuniard/projects/udemy_spark/customer-orders.csv")
rdd = lines.map(parseLine)
customerTotalDollarAmt = rdd.reduceByKey(lambda x, y: x + y)
results = customerTotalDollarAmt.collect()

for result in sorted(results):
    print(result)