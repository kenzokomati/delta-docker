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

df = spark.read.csv("data/sales_data.csv", header=True, schema=customSchema, quote='"', escape='"')

# df.printSchema()
df.show(truncate=False)


# array de strings 

discounts_schema = ArrayType(
    MapType(
        StringType(),
        DoubleType()
        )
)


payment_schema = ArrayType(StringType())

# Aplicar parsing JSON nas colunas
df = df.withColumn("sale_date", to_timestamp("sale_date", "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("discounts", from_json("discounts", discounts_schema))
df = df.withColumn("payment_methods", from_json("payment_methods", payment_schema))

# df.select("payment_methods").show(truncate=False)
# df.select("discounts").show(truncate=False)
# df.printSchema()
df.show(truncate=False)


# fazer explode da coluna em duas colunas
df = df.select("*", explode("discounts"))
df.show(truncate=False)
# df_exploded_discounts = df_exploded_discounts.select("*", explode("discount").alias("discount_name","discount_percent"))
# df_exploded_discounts.show(truncate=False)
# df.select("*", explode("payment_methods").alias("payment_method")).show()


# df = sqlContext.read \
#     .format('com.databricks.spark.csv') \
#     .options(header='true') \
#     .load('cars.csv', schema = customSchema)

# df.select('year', 'model').write \
#     .format('com.databricks.spark.csv') \
#     .save('newcars.csv')