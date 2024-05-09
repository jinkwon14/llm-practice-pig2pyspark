# filename: pyspark\_code.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Pig-to-PySpark") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Load the data from a CSV file
transactions = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("data/sample1.csv") \
    .toDF("depStore", "date", "amount")

# Filter transactions to include only those where the amount is greater than 200
high_value_transactions = transactions.filter(transactions.amount > 200)

# Group the transactions by store
grouped_by_store = high_value_transactions.groupBy("depStore")

# Calculate total and average sales per depStore
sales_summary = grouped_by_store \
    .agg({"amount": "sum"}, {"amount": "avg"}) \
    .toDF("depStore", "total_sales", "average_sales")

# Store the summary in a CSV file
sales_summary.write \
    .format("csv") \
    .option("header", "true") \
    .save("output/sales_summary.csv")

# Optional: Just for demonstration, store filtered data to another directory
high_value_transactions.write \
    .format("csv") \
    .option("header", "true") \
    .save("output/high_value_transactions.csv")