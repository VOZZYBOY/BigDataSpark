from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL_Neo4j_Reports") \
        .config("spark.jars", "/opt/spark-apps/postgresql-42.7.3.jar,/opt/spark-apps/neo4j-connector-apache-spark_2.12-5.0.1_for_spark_3.jar") \
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

def save_to_neo4j(df, node_label):
    df.write \
        .format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://neo4j:7687") \
        .option("authentication.basic.username", "neo4j") \
        .option("authentication.basic.password", "password") \
        .option("labels", node_label) \
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
        
        product_sales = fact_sales.join(dim_products, "product_id")
        customer_sales = fact_sales.join(dim_customers, "customer_id")
        store_sales = fact_sales.join(dim_stores, "store_id")
        supplier_sales = fact_sales.join(dim_suppliers, "supplier_id")
        
        top_products = product_sales \
            .groupBy("product_id", "product_name", "product_category") \
            .agg(sum("sale_total_price").alias("total_revenue")) \
            .orderBy(desc("total_revenue")) \
            .limit(10)
        
        top_customers = customer_sales \
            .groupBy("customer_id", "customer_first_name", "customer_last_name") \
            .agg(sum("sale_total_price").alias("total_spent")) \
            .orderBy(desc("total_spent")) \
            .limit(10)
        
        store_performance = store_sales \
            .groupBy("store_id", "store_name", "store_city") \
            .agg(sum("sale_total_price").alias("total_revenue")) \
            .orderBy(desc("total_revenue"))
        
        supplier_performance = supplier_sales \
            .groupBy("supplier_id", "supplier_name") \
            .agg(sum("sale_total_price").alias("total_revenue")) \
            .orderBy(desc("total_revenue"))
        
        product_ratings = dim_products \
            .select("product_id", "product_name", "product_rating", "product_reviews") \
            .orderBy(desc("product_rating"))
        
        customer_relationships = customer_sales \
            .select("customer_id", "customer_country", "sale_total_price")
        
        save_to_neo4j(top_products, "TopProduct")
        save_to_neo4j(top_customers, "TopCustomer")
        save_to_neo4j(store_performance, "StorePerformance")
        save_to_neo4j(supplier_performance, "SupplierPerformance")
        save_to_neo4j(product_ratings, "ProductRating")
        save_to_neo4j(customer_relationships, "CustomerRelation")
        
        print("Neo4j reports generated successfully!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
