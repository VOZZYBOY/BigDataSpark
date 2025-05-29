from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("Load_CSV_to_PostgreSQL") \
        .config("spark.jars", "/opt/spark-apps/postgresql-42.7.3.jar") \
        .getOrCreate()

def define_schema():
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("customer_first_name", StringType(), True),
        StructField("customer_last_name", StringType(), True),
        StructField("customer_age", IntegerType(), True),
        StructField("customer_email", StringType(), True),
        StructField("customer_country", StringType(), True),
        StructField("customer_postal_code", StringType(), True),
        StructField("customer_pet_type", StringType(), True),
        StructField("customer_pet_name", StringType(), True),
        StructField("customer_pet_breed", StringType(), True),
        StructField("seller_first_name", StringType(), True),
        StructField("seller_last_name", StringType(), True),
        StructField("seller_email", StringType(), True),
        StructField("seller_country", StringType(), True),
        StructField("seller_postal_code", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_price", DecimalType(10,2), True),
        StructField("product_quantity", IntegerType(), True),
        StructField("sale_date", StringType(), True),
        StructField("sale_customer_id", IntegerType(), True),
        StructField("sale_seller_id", IntegerType(), True),
        StructField("sale_product_id", IntegerType(), True),
        StructField("sale_quantity", IntegerType(), True),
        StructField("sale_total_price", DecimalType(10,2), True),
        StructField("store_name", StringType(), True),
        StructField("store_location", StringType(), True),
        StructField("store_city", StringType(), True),
        StructField("store_state", StringType(), True),
        StructField("store_country", StringType(), True),
        StructField("store_phone", StringType(), True),
        StructField("store_email", StringType(), True),
        StructField("pet_category", StringType(), True),
        StructField("product_weight", DecimalType(10,2), True),
        StructField("product_color", StringType(), True),
        StructField("product_size", StringType(), True),
        StructField("product_brand", StringType(), True),
        StructField("product_material", StringType(), True),
        StructField("product_description", StringType(), True),
        StructField("product_rating", DecimalType(3,2), True),
        StructField("product_reviews", IntegerType(), True),
        StructField("product_release_date", StringType(), True),
        StructField("product_expiry_date", StringType(), True),
        StructField("supplier_name", StringType(), True),
        StructField("supplier_contact", StringType(), True),
        StructField("supplier_email", StringType(), True),
        StructField("supplier_phone", StringType(), True),
        StructField("supplier_address", StringType(), True),
        StructField("supplier_city", StringType(), True),
        StructField("supplier_country", StringType(), True)
    ])

def load_csv_files(spark, schema):
    csv_files = [
        "/data/MOCK_DATA.csv",
        "/data/MOCK_DATA (1).csv",
        "/data/MOCK_DATA (2).csv",
        "/data/MOCK_DATA (3).csv",
        "/data/MOCK_DATA (4).csv",
        "/data/MOCK_DATA (5).csv",
        "/data/MOCK_DATA (6).csv",
        "/data/MOCK_DATA (7).csv",
        "/data/MOCK_DATA (8).csv",
        "/data/MOCK_DATA (9).csv"
    ]
    
    all_data = None
    
    for file_path in csv_files:
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .load(file_path)
        
        df = df.withColumn("sale_date", to_date(col("sale_date"), "M/d/yyyy")) \
               .withColumn("product_release_date", to_date(col("product_release_date"), "M/d/yyyy")) \
               .withColumn("product_expiry_date", to_date(col("product_expiry_date"), "M/d/yyyy"))
        
        if all_data is None:
            all_data = df
        else:
            all_data = all_data.union(df)
    
    return all_data

def save_to_postgres(df):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/bigdata") \
        .option("dbtable", "mock_data") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

def main():
    spark = create_spark_session()
    
    try:
        schema = define_schema()
        df = load_csv_files(spark, schema)
        
        print(f"Total records loaded: {df.count()}")
        
        save_to_postgres(df)
        
        print("Data successfully loaded to PostgreSQL!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
