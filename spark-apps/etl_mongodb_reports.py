from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL_MongoDB_Reports") \
        .config("spark.jars", "/opt/spark-apps/postgresql-42.7.3.jar,/opt/spark-apps/mongo-spark-connector_2.12-10.1.1.jar") \
        .config("spark.mongodb.read.connection.uri", "mongodb://admin:password@mongodb:27017/bigdata.reports?authSource=admin") \
        .config("spark.mongodb.write.connection.uri", "mongodb://admin:password@mongodb:27017/bigdata.reports?authSource=admin") \
        .getOrCreate()

def load_star_schema_data(spark):
    tables = {}
    
    for table in ["dim_customers", "dim_sellers", "dim_products", "dim_stores", "dim_suppliers", "dim_time", "fact_sales"]:
        tables[table] = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/bigdata") \
            .option("dbtable", table) \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .load()
    
    return tables

def save_to_mongodb(df, collection_name):
    df.write \
        .format("mongodb") \
        .option("collection", collection_name) \
        .mode("overwrite") \
        .save()

def main():
    spark = create_spark_session()
    
    try:
        tables = load_star_schema_data(spark)
        
        fact_sales = tables["fact_sales"]
        dim_products = tables["dim_products"]
        dim_customers = tables["dim_customers"]
        dim_stores = tables["dim_stores"]
        dim_suppliers = tables["dim_suppliers"]
        dim_time = tables["dim_time"]
        
        product_sales = fact_sales.join(dim_products, "product_id")
        customer_sales = fact_sales.join(dim_customers, "customer_id")
        store_sales = fact_sales.join(dim_stores, "store_id")
        supplier_sales = fact_sales.join(dim_suppliers, "supplier_id")
        time_sales = fact_sales.join(dim_time, "sale_date")
        
        product_vitrina = product_sales \
            .groupBy("product_id", "product_name", "product_category") \
            .agg(
                sum("sale_quantity").alias("total_quantity"),
                sum("sale_total_price").alias("total_revenue"),
                avg("product_rating").alias("avg_rating"),
                max("product_reviews").alias("review_count")
            ) \
            .withColumn("report_type", lit("product_sales"))
        
        customer_vitrina = customer_sales \
            .groupBy("customer_id", "customer_first_name", "customer_last_name", "customer_country") \
            .agg(
                sum("sale_total_price").alias("total_spent"),
                count("*").alias("order_count"),
                avg("sale_total_price").alias("avg_order_value")
            ) \
            .withColumn("report_type", lit("customer_analysis"))
        
        time_vitrina = time_sales \
            .groupBy("year", "month") \
            .agg(
                sum("sale_total_price").alias("revenue"),
                count("*").alias("order_count"),
                avg("sale_total_price").alias("avg_order_size")
            ) \
            .withColumn("report_type", lit("time_trends"))
        
        store_vitrina = store_sales \
            .groupBy("store_id", "store_name", "store_city", "store_country") \
            .agg(
                sum("sale_total_price").alias("total_revenue"),
                count("*").alias("order_count"),
                avg("sale_total_price").alias("avg_order_value")
            ) \
            .withColumn("report_type", lit("store_performance"))
        
        supplier_vitrina = supplier_sales \
            .join(dim_products, "product_id") \
            .groupBy("supplier_id", "supplier_name", "supplier_country") \
            .agg(
                sum("sale_total_price").alias("total_revenue"),
                avg("product_price").alias("avg_product_price"),
                count("*").alias("product_count")
            ) \
            .withColumn("report_type", lit("supplier_analysis"))
        
        quality_vitrina = dim_products \
            .join(product_sales.groupBy("product_id").agg(sum("sale_quantity").alias("sales_volume")), "product_id") \
            .select(
                "product_id",
                "product_name",
                "product_rating",
                "product_reviews",
                "sales_volume"
            ) \
            .withColumn("rating_category", 
                when(col("product_rating") >= 4.0, "High")
                .when(col("product_rating") >= 3.0, "Medium")
                .otherwise("Low")
            ) \
            .withColumn("report_type", lit("quality_analysis"))
        
        save_to_mongodb(product_vitrina, "product_vitrina")
        save_to_mongodb(customer_vitrina, "customer_vitrina")
        save_to_mongodb(time_vitrina, "time_vitrina")
        save_to_mongodb(store_vitrina, "store_vitrina")
        save_to_mongodb(supplier_vitrina, "supplier_vitrina")
        save_to_mongodb(quality_vitrina, "quality_vitrina")
        
        print("MongoDB reports generated successfully!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
