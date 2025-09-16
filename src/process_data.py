import pyspark
from delta import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, col, explode, split

# ----------------------------------

builder = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# Columns : transaction_id,customer_id,product_category,
# product_name,sale_date,price,quantity,discounts,payment_methods

# Schema definition
customSchema = StructType([ \
    StructField("transaction_id", IntegerType(), True), \
    StructField("customer_id", StringType(), True), \
    StructField("product_category", StringType(), True), \
    StructField("product_name", StringType(), True), \
    StructField("sale_date", StringType(), True), \
    StructField("price", DecimalType(10,4), True), \
    StructField("quantity", IntegerType(), True), \
    StructField("discounts", StringType(), True), \
    StructField("payment_methods", StringType(), True)]) 

# Reading CSV with custom schema 
df = spark.read.csv("data/sales_data.csv", header=True, schema=customSchema, quote='"', escape='"')

df.show(truncate=False)

# Defining schema for string columns to parse them
discounts_schema = ArrayType(
    StructType([
        StructField("type", StringType(), True),
        StructField("value", DoubleType(), True)
    ])
)

payment_schema = ArrayType(StringType())

# Parsing string columns and converting sale_date to timestamp
df = df.withColumn("sale_date", to_timestamp("sale_date", "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("discounts", from_json("discounts", discounts_schema))
df = df.withColumn("payment_methods", from_json("payment_methods", payment_schema))

df.show(truncate=False)
df.printSchema()


# Explode arrays
## Exploding discounts array into multiple rows 
df_exploded_discounts = df.select("*", explode("discounts")
                                  .alias("discount")) \
                                  .drop("discounts")

## Expanding discount struct fields and renaming them
df_exploded_discounts = df_exploded_discounts.select("*", "discount.type","discount.value")\
                                             .withColumnRenamed("type","discount_type") \
                                             .withColumnRenamed("value","discount_percent") \
                                             .drop("discount")

## Exploding payment_methods array into multiple rows                                            
df_exploded = df_exploded_discounts.select("*", explode("payment_methods")
                                           .alias("payment_method")) \
                                           .drop("payment_methods")
df_exploded.show()