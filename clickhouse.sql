-- DataMartProducts

CREATE TABLE IF NOT EXISTS mart_products_top
(
    id Int64,
    name String,
    quantity_sold Int64
)
ENGINE = MergeTree()
ORDER BY (quantity_sold);

CREATE TABLE IF NOT EXISTS mart_products_revenue
(
    category_id Int64,
    category String,
    revenue Float64
)
ENGINE = MergeTree()
ORDER BY (revenue);


-- DataMartCustomers

CREATE TABLE IF NOT EXISTS mart_customers_top
(
    id Int64,
    fname String,
    lname String,
    spent_total Float64
)
ENGINE = MergeTree()
ORDER BY (spent_total);

CREATE TABLE IF NOT EXISTS mart_customers_by_country
(
    country_id Int64,
    country String,
    customers_count Int64
)
ENGINE = MergeTree()
ORDER BY (customers_count);

CREATE TABLE IF NOT EXISTS mart_customers_avg_check
(
    id Int64,
    fname String,
    lname String,
    avg_check Float64
)
ENGINE = MergeTree()
ORDER BY (avg_check);


-- DataMartTimes

CREATE TABLE IF NOT EXISTS mart_yearly_sales
(
    year Int32,
    revenue Float64,
    items_sold Int64
)
ENGINE = MergeTree()
ORDER BY (year);

CREATE TABLE IF NOT EXISTS mart_monthly_sales
(
    year Int32,
    month Int32,
    revenue Float64,
    items_sold Int64
)
ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS mart_monthly_comparison
(
    year Int32,
    month Int32,
    revenue Float64,
    prev_revenue Float64,
    growth Float64
)
ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS mart_quarterly_sales
(
    year Int32,
    quarter Int32,
    revenue Float64
)
ENGINE = MergeTree()
ORDER BY (year, quarter);

CREATE TABLE IF NOT EXISTS mart_avg_order
(
    year Int32,
    month Int32,
    avg_value Float64,
    avg_items Float64,
    orders Int32
)
ENGINE = MergeTree()
ORDER BY (year, month);


-- DataMartStores

CREATE TABLE IF NOT EXISTS mart_stores_top
(
    id Int64,
    store String,
    revenue Float64
)
ENGINE = MergeTree()
ORDER BY (revenue);

CREATE TABLE IF NOT EXISTS mart_sales_by_country
(
    country_id Int64,
    country String,
    revenue Float64,
    items_sold Int64,
    orders Int64
)
ENGINE = MergeTree()
ORDER BY (revenue);

CREATE TABLE IF NOT EXISTS mart_sales_by_city
(
    country_id Int64,
    country String,
    city_id Int64,
    city String,
    revenue Float64,
    items_sold Int64,
    orders Int64
)
ENGINE = MergeTree()
ORDER BY (revenue);

CREATE TABLE IF NOT EXISTS mart_avg_receipt
(
    store_id Int64,
    store String,
    city_id Int64,
    avg_receipt Float64
)
ENGINE = MergeTree()
ORDER BY (avg_receipt);


-- DataMartSuppliers

CREATE TABLE IF NOT EXISTS mart_suppliers_top
(
    id Int64,
    supplier String,
    revenue Float64
)
ENGINE = MergeTree()
ORDER BY (revenue);

CREATE TABLE IF NOT EXISTS mart_suppliers_avg_price
(
    id Int64,
    supplier String,
    avg_price Float64
)
ENGINE = MergeTree()
ORDER BY (avg_price);

CREATE TABLE IF NOT EXISTS mart_suppliers_distribution
(
    country_id Int64,
    country String,
    revenue Float64,
    items_sold Int64,
    orders Int64,
    suppliers Int64
)
ENGINE = MergeTree()
ORDER BY (revenue);


-- DataMartQuality

CREATE TABLE IF NOT EXISTS mart_products_best
(
    id Int64,
    name String,
    rating Float64
)
ENGINE = MergeTree()
ORDER BY (rating);

CREATE TABLE IF NOT EXISTS mart_products_worst
(
    id Int64,
    name String,
    rating Float64
)
ENGINE = MergeTree()
ORDER BY (rating);

CREATE TABLE IF NOT EXISTS mart_quality_correlation
(
    metric String,
    corr_matrix Float64,
    corr_sql Float64
)
ENGINE = MergeTree()
ORDER BY (corr_matrix, corr_sql);

CREATE TABLE IF NOT EXISTS mart_products_reviews
(
    id Int64,
    name String,
    reviews Int64
)
ENGINE = MergeTree()
ORDER BY (reviews);
