# filename: pyspark_script.py
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import os

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"

spark = SparkSession.builder.appName('PigToPySpark').getOrCreate()

# Load input data into a DataFrame named 'sales_data'
input_path = "/workspace/data/"
sales_data_csv = os.path.join(input_path, "sales_data.csv")

sales_data = spark.read.option("header", "true").csv(sales_data_csv)

# ... Continue with the rest of your PySpark code.