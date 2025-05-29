CREATE KEYSPACE IF NOT EXISTS bigdata 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE bigdata;

CREATE TABLE IF NOT EXISTS top_products (
    product_id int,
    product_name text,
    product_category text,
    total_quantity bigint,
    total_revenue decimal,
    PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS top_customers (
    customer_id int,
    customer_first_name text,
    customer_last_name text,
    total_spent decimal,
    PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS monthly_sales (
    year int,
    month int,
    monthly_revenue decimal,
    PRIMARY KEY (year, month)
);

CREATE TABLE IF NOT EXISTS top_stores (
    store_id int,
    store_name text,
    total_revenue decimal,
    PRIMARY KEY (store_id)
);

CREATE TABLE IF NOT EXISTS top_suppliers (
    supplier_id int,
    supplier_name text,
    total_revenue decimal,
    PRIMARY KEY (supplier_id)
);

CREATE TABLE IF NOT EXISTS product_quality (
    product_id int,
    product_name text,
    product_rating decimal,
    product_reviews int,
    PRIMARY KEY (product_id)
);
