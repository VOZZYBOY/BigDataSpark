from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL_Valkey_Reports") \
        .config("spark.jars", "/opt/spark-apps/postgresql-42.7.3.jar") \
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

def create_redis_format(df, key_prefix):
    return df.rdd.map(lambda row: {
        "key": f"{key_prefix}:{row[0]}",
        "value": json.dumps(row.asDict())
    }).toDF(["key", "value"])

def save_to_valkey(df, key_prefix):
    redis_df = create_redis_format(df, key_prefix)
    
    redis_df.write \
        .format("org.apache.spark.sql.redis") \
        .option("host", "valkey") \
        .option("port", "6379") \
        .option("keys.pattern", "key") \
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
        
        top_products = product_sales \
            .groupBy("product_id", "product_name", "product_category") \
            .agg(
                sum("sale_quantity").alias("total_quantity"),
                sum("sale_total_price").alias("total_revenue")
            ) \
            .orderBy(desc("total_quantity")) \
            .limit(10) \
            .withColumn("rank", row_number().over(Window.orderBy(desc("total_quantity"))))
        
        top_customers = customer_sales \
            .groupBy("customer_id", "customer_first_name", "customer_last_name") \
            .agg(sum("sale_total_price").alias("total_spent")) \
            .orderBy(desc("total_spent")) \
            .limit(10) \
            .withColumn("rank", row_number().over(Window.orderBy(desc("total_spent"))))
        
        monthly_trends = time_sales \
            .groupBy("year", "month") \
            .agg(sum("sale_total_price").alias("monthly_revenue")) \
            .orderBy("year", "month") \
            .withColumn("period_id", concat(col("year"), lit("-"), col("month")))
        
        store_performance = store_sales \
            .groupBy("store_id", "store_name") \
            .agg(sum("sale_total_price").alias("total_revenue")) \
            .orderBy(desc("total_revenue"))
        
        supplier_performance = supplier_sales \
            .join(dim_products, "product_id") \
            .groupBy("supplier_id", "supplier_name") \
            .agg(
                sum("sale_total_price").alias("total_revenue"),
                avg("product_price").alias("avg_price")
            ) \
            .orderBy(desc("total_revenue"))
        
        product_quality = dim_products \
            .select("product_id", "product_name", "product_rating", "product_reviews") \
            .orderBy(desc("product_rating"))
        
        summary_stats = spark.createDataFrame([
            ("total_sales", float(fact_sales.agg(sum("sale_total_price")).collect()[0][0])),
            ("total_orders", fact_sales.count()),
            ("avg_order_value", float(fact_sales.agg(avg("sale_total_price")).collect()[0][0])),
            ("unique_customers", fact_sales.select("customer_id").distinct().count()),
            ("unique_products", fact_sales.select("product_id").distinct().count())
        ], ["metric", "value"])
        
        save_to_valkey(top_products, "top_products")
        save_to_valkey(top_customers, "top_customers")
        save_to_valkey(monthly_trends, "monthly_trends")
        save_to_valkey(store_performance, "store_performance")
        save_to_valkey(supplier_performance, "supplier_performance")
        save_to_valkey(product_quality, "product_quality")
        save_to_valkey(summary_stats, "summary_stats")
        
        print("Valkey reports generated successfully!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
