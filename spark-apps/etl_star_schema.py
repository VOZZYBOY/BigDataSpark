from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL_Star_Schema") \
        .config("spark.jars", "/opt/spark-apps/postgresql-42.7.3.jar") \
        .getOrCreate()

def load_data_from_postgres(spark):
    return spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/bigdata") \
        .option("dbtable", "mock_data") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()

def create_dimension_tables(df):
    dim_customers = df.select(
        col("sale_customer_id").alias("customer_id"),
        "customer_first_name",
        "customer_last_name", 
        "customer_age",
        "customer_email",
        "customer_country",
        "customer_postal_code",
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed"
    ).distinct()
    
    dim_sellers = df.select(
        col("sale_seller_id").alias("seller_id"),
        "seller_first_name",
        "seller_last_name",
        "seller_email", 
        "seller_country",
        "seller_postal_code"
    ).distinct()
    
    dim_products = df.select(
        col("sale_product_id").alias("product_id"),
        "product_name",
        "product_category",
        "product_price",
        "product_weight",
        "product_color",
        "product_size",
        "product_brand",
        "product_material",
        "product_description",
        "product_rating",
        "product_reviews",
        "product_release_date",
        "product_expiry_date"
    ).distinct()
    
    dim_stores = df.select(
        "store_name",
        "store_location",
        "store_city", 
        "store_state",
        "store_country",
        "store_phone",
        "store_email"
    ).distinct().withColumn("store_id", row_number().over(Window.orderBy("store_name")))
    
    dim_suppliers = df.select(
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country"
    ).distinct().withColumn("supplier_id", row_number().over(Window.orderBy("supplier_name")))
    
    dim_time = df.select("sale_date").distinct() \
        .withColumn("year", year("sale_date")) \
        .withColumn("month", month("sale_date")) \
        .withColumn("day", dayofmonth("sale_date")) \
        .withColumn("quarter", quarter("sale_date")) \
        .withColumn("week", weekofyear("sale_date"))
    
    return dim_customers, dim_sellers, dim_products, dim_stores, dim_suppliers, dim_time

def create_fact_table(df, dim_stores, dim_suppliers):
    stores_with_id = dim_stores.select("store_name", "store_id")
    suppliers_with_id = dim_suppliers.select("supplier_name", "supplier_id")
    
    fact_sales = df.select(
        "id",
        "sale_customer_id",
        "sale_seller_id", 
        "sale_product_id",
        "sale_date",
        "sale_quantity",
        "sale_total_price",
        "store_name",
        "supplier_name"
    ).join(stores_with_id, "store_name") \
     .join(suppliers_with_id, "supplier_name") \
     .select(
        "id",
        col("sale_customer_id").alias("customer_id"),
        col("sale_seller_id").alias("seller_id"),
        col("sale_product_id").alias("product_id"),
        "store_id",
        "supplier_id",
        "sale_date",
        "sale_quantity",
        "sale_total_price"
    )
    
    return fact_sales

def save_to_postgres(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/bigdata") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

def main():
    spark = create_spark_session()
    
    try:
        df = load_data_from_postgres(spark)
        
        dim_customers, dim_sellers, dim_products, dim_stores, dim_suppliers, dim_time = create_dimension_tables(df)
        fact_sales = create_fact_table(df, dim_stores, dim_suppliers)
        
        save_to_postgres(dim_customers, "dim_customers")
        save_to_postgres(dim_sellers, "dim_sellers") 
        save_to_postgres(dim_products, "dim_products")
        save_to_postgres(dim_stores, "dim_stores")
        save_to_postgres(dim_suppliers, "dim_suppliers")
        save_to_postgres(dim_time, "dim_time")
        save_to_postgres(fact_sales, "fact_sales")
        
        print("ETL process completed successfully!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
