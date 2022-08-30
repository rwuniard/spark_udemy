from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///Users/rwuniard/projects/udemy_spark//1800.csv")
# parseLine return stationID, entryType, temperature.
parsedLines = lines.map(parseLine)
# Filter out all but TMIN entries.
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
# stationID, temperature.
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# Reduce by stationID retaining the minimum temperature found.
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
