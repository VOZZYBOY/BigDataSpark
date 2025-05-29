from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL_ClickHouse_Reports") \
        .config("spark.jars", "/opt/spark-apps/postgresql-42.7.3.jar,/opt/spark-apps/clickhouse-jdbc-0.4.6.jar") \
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

def create_product_reports(tables):
    fact_sales = tables["fact_sales"]
    dim_products = tables["dim_products"]
    
    product_sales = fact_sales.join(dim_products, "product_id")
    
    top_selling_products = product_sales \
        .groupBy("product_id", "product_name", "product_category") \
        .agg(
            sum("sale_quantity").alias("total_quantity"),
            sum("sale_total_price").alias("total_revenue")
        ) \
        .orderBy(desc("total_quantity")) \
        .limit(10)
    
    revenue_by_category = product_sales \
        .groupBy("product_category") \
        .agg(sum("sale_total_price").alias("total_revenue")) \
        .orderBy(desc("total_revenue"))
    
    product_ratings = dim_products \
        .select("product_id", "product_name", "product_rating", "product_reviews") \
        .orderBy(desc("product_rating"))
    
    return top_selling_products, revenue_by_category, product_ratings

def create_customer_reports(tables):
    fact_sales = tables["fact_sales"]
    dim_customers = tables["dim_customers"]
    
    customer_sales = fact_sales.join(dim_customers, "customer_id")
    
    top_customers = customer_sales \
        .groupBy("customer_id", "customer_first_name", "customer_last_name", "customer_country") \
        .agg(
            sum("sale_total_price").alias("total_spent"),
            count("*").alias("order_count")
        ) \
        .orderBy(desc("total_spent")) \
        .limit(10)
    
    customers_by_country = customer_sales \
        .groupBy("customer_country") \
        .agg(
            countDistinct("customer_id").alias("customer_count"),
            sum("sale_total_price").alias("total_revenue")
        ) \
        .orderBy(desc("customer_count"))
    
    avg_order_value = customer_sales \
        .groupBy("customer_id", "customer_first_name", "customer_last_name") \
        .agg(avg("sale_total_price").alias("avg_order_value")) \
        .orderBy(desc("avg_order_value"))
    
    return top_customers, customers_by_country, avg_order_value

def create_time_reports(tables):
    fact_sales = tables["fact_sales"]
    dim_time = tables["dim_time"]
    
    time_sales = fact_sales.join(dim_time, "sale_date")
    
    monthly_trends = time_sales \
        .groupBy("year", "month") \
        .agg(
            sum("sale_total_price").alias("monthly_revenue"),
            count("*").alias("order_count")
        ) \
        .orderBy("year", "month")
    
    yearly_trends = time_sales \
        .groupBy("year") \
        .agg(
            sum("sale_total_price").alias("yearly_revenue"),
            count("*").alias("order_count")
        ) \
        .orderBy("year")
    
    avg_order_by_month = time_sales \
        .groupBy("year", "month") \
        .agg(avg("sale_total_price").alias("avg_order_value")) \
        .orderBy("year", "month")
    
    return monthly_trends, yearly_trends, avg_order_by_month

def create_store_reports(tables):
    fact_sales = tables["fact_sales"]
    dim_stores = tables["dim_stores"]
    
    store_sales = fact_sales.join(dim_stores, "store_id")
    
    top_stores = store_sales \
        .groupBy("store_id", "store_name", "store_city", "store_country") \
        .agg(sum("sale_total_price").alias("total_revenue")) \
        .orderBy(desc("total_revenue")) \
        .limit(5)
    
    sales_by_location = store_sales \
        .groupBy("store_city", "store_country") \
        .agg(
            sum("sale_total_price").alias("total_revenue"),
            count("*").alias("order_count")
        ) \
        .orderBy(desc("total_revenue"))
    
    store_avg_order = store_sales \
        .groupBy("store_id", "store_name") \
        .agg(avg("sale_total_price").alias("avg_order_value")) \
        .orderBy(desc("avg_order_value"))
    
    return top_stores, sales_by_location, store_avg_order

def create_supplier_reports(tables):
    fact_sales = tables["fact_sales"]
    dim_suppliers = tables["dim_suppliers"]
    dim_products = tables["dim_products"]
    
    supplier_sales = fact_sales.join(dim_suppliers, "supplier_id").join(dim_products, "product_id")
    
    top_suppliers = supplier_sales \
        .groupBy("supplier_id", "supplier_name", "supplier_country") \
        .agg(sum("sale_total_price").alias("total_revenue")) \
        .orderBy(desc("total_revenue")) \
        .limit(5)
    
    supplier_avg_price = supplier_sales \
        .groupBy("supplier_id", "supplier_name") \
        .agg(avg("product_price").alias("avg_product_price")) \
        .orderBy(desc("avg_product_price"))
    
    supplier_by_country = supplier_sales \
        .groupBy("supplier_country") \
        .agg(
            sum("sale_total_price").alias("total_revenue"),
            countDistinct("supplier_id").alias("supplier_count")
        ) \
        .orderBy(desc("total_revenue"))
    
    return top_suppliers, supplier_avg_price, supplier_by_country

def create_quality_reports(tables):
    dim_products = tables["dim_products"]
    fact_sales = tables["fact_sales"]
    
    product_sales = fact_sales.join(dim_products, "product_id")
    
    highest_rated = dim_products \
        .select("product_id", "product_name", "product_rating", "product_reviews") \
        .orderBy(desc("product_rating")) \
        .limit(10)
    
    lowest_rated = dim_products \
        .select("product_id", "product_name", "product_rating", "product_reviews") \
        .orderBy(asc("product_rating")) \
        .limit(10)
    
    rating_sales_correlation = product_sales \
        .groupBy("product_id", "product_name", "product_rating") \
        .agg(sum("sale_quantity").alias("total_sales")) \
        .orderBy(desc("product_rating"))
    
    most_reviewed = dim_products \
        .select("product_id", "product_name", "product_reviews") \
        .orderBy(desc("product_reviews")) \
        .limit(10)
    
    return highest_rated, lowest_rated, rating_sales_correlation, most_reviewed

def save_to_clickhouse(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("dbtable", table_name) \
        .option("user", "default") \
        .option("password", "password") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("overwrite") \
        .save()

def main():
    spark = create_spark_session()
    
    try:
        tables = load_star_schema_data(spark)
        
        top_products, revenue_by_category, product_ratings = create_product_reports(tables)
        top_customers, customers_by_country, avg_customer_order = create_customer_reports(tables)
        monthly_trends, yearly_trends, avg_order_by_month = create_time_reports(tables)
        top_stores, sales_by_location, store_avg_order = create_store_reports(tables)
        top_suppliers, supplier_avg_price, supplier_by_country = create_supplier_reports(tables)
        highest_rated, lowest_rated, rating_correlation, most_reviewed = create_quality_reports(tables)
        
        save_to_clickhouse(top_products, "report_top_products")
        save_to_clickhouse(revenue_by_category, "report_revenue_by_category")
        save_to_clickhouse(product_ratings, "report_product_ratings")
        
        save_to_clickhouse(top_customers, "report_top_customers")
        save_to_clickhouse(customers_by_country, "report_customers_by_country")
        save_to_clickhouse(avg_customer_order, "report_customer_avg_order")
        
        save_to_clickhouse(monthly_trends, "report_monthly_trends")
        save_to_clickhouse(yearly_trends, "report_yearly_trends")
        save_to_clickhouse(avg_order_by_month, "report_avg_order_by_month")
        
        save_to_clickhouse(top_stores, "report_top_stores")
        save_to_clickhouse(sales_by_location, "report_sales_by_location")
        save_to_clickhouse(store_avg_order, "report_store_avg_order")
        
        save_to_clickhouse(top_suppliers, "report_top_suppliers")
        save_to_clickhouse(supplier_avg_price, "report_supplier_avg_price")
        save_to_clickhouse(supplier_by_country, "report_supplier_by_country")
        
        save_to_clickhouse(highest_rated, "report_highest_rated")
        save_to_clickhouse(lowest_rated, "report_lowest_rated")
        save_to_clickhouse(rating_correlation, "report_rating_correlation")
        save_to_clickhouse(most_reviewed, "report_most_reviewed")
        
        print("ClickHouse reports generated successfully!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
