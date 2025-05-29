from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL_Cassandra_Reports") \
        .config("spark.jars", "/opt/spark-apps/postgresql-42.7.3.jar") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
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

def save_to_cassandra(df, keyspace, table_name):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", keyspace) \
        .option("table", table_name) \
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
        supplier_sales = fact_sales.join(dim_suppliers, "supplier_id").join(dim_products, "product_id")
        time_sales = fact_sales.join(dim_time, "sale_date")
        
        top_products = product_sales \
            .groupBy("product_id", "product_name", "product_category") \
            .agg(
                sum("sale_quantity").alias("total_quantity"),
                sum("sale_total_price").alias("total_revenue")
            ) \
            .orderBy(desc("total_quantity")) \
            .limit(10)
        
        top_customers = customer_sales \
            .groupBy("customer_id", "customer_first_name", "customer_last_name") \
            .agg(sum("sale_total_price").alias("total_spent")) \
            .orderBy(desc("total_spent")) \
            .limit(10)
        
        monthly_sales = time_sales \
            .groupBy("year", "month") \
            .agg(sum("sale_total_price").alias("monthly_revenue")) \
            .orderBy("year", "month")
        
        top_stores = store_sales \
            .groupBy("store_id", "store_name") \
            .agg(sum("sale_total_price").alias("total_revenue")) \
            .orderBy(desc("total_revenue")) \
            .limit(5)
        
        top_suppliers = supplier_sales \
            .groupBy("supplier_id", "supplier_name") \
            .agg(sum("sale_total_price").alias("total_revenue")) \
            .orderBy(desc("total_revenue")) \
            .limit(5)
        
        product_quality = dim_products \
            .select("product_id", "product_name", "product_rating", "product_reviews") \
            .orderBy(desc("product_rating"))
        
        save_to_cassandra(top_products, "bigdata", "top_products")
        save_to_cassandra(top_customers, "bigdata", "top_customers")
        save_to_cassandra(monthly_sales, "bigdata", "monthly_sales")
        save_to_cassandra(top_stores, "bigdata", "top_stores")
        save_to_cassandra(top_suppliers, "bigdata", "top_suppliers")
        save_to_cassandra(product_quality, "bigdata", "product_quality")
        
        print("Cassandra reports generated successfully!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
