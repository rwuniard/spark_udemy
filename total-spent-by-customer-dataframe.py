from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").master("local[*]").getOrCreate()

schema = StructType([ \
                     StructField("custID", IntegerType(), True), \
                     StructField("SKU", IntegerType(), True), \
                     StructField("dollar_amt", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("file:///Users/rwuniard/projects/udemy_spark/customer-orders.csv")
df.printSchema()

# Group by custID and aggregate the dollar_amt -> put it into a column called total_spent.
custTotalSpent = df.groupBy("custID").agg(func.sum("dollar_amt").alias("total_spent"))
# sort the dataframe by total_spent.
custTotalSpentSorted = custTotalSpent.sort("total_spent", ascending=False)

# print the result with the format "custID: \tab $total_spent with 2 decimal places".
for cust in custTotalSpentSorted.collect():
    print("{:d}\t${:.2f}".format(cust[0], cust[1]))

    
spark.stop()

                                                  