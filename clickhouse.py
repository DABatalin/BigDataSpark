from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count as _count, avg as _avg, asc, desc, year, month, quarter, lag, countDistinct, corr
from pyspark.sql.window import Window
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from time import sleep


spark = SparkSession.builder \
        .appName("BD Spark ClickHouse") \
        .config("spark.jars", "postgresql-42.6.0.jar,clickhouse-jdbc-0.4.6.jar") \
        .getOrCreate()
    
pg_url = "jdbc:postgresql://localhost:5432/spark_db"
pg_opts = {"user": "spark_user", "password": "spark_password", "driver": "org.postgresql.Driver"}

ch_url = "jdbc:clickhouse://localhost:8123/default"
ch_opts = {"driver": "com.clickhouse.jdbc.ClickHouseDriver", "user": "custom_user", "password": "custom_password"}

df_dates = spark.read.jdbc(url=pg_url, table="d_dates", properties=pg_opts)
df_cities = spark.read.jdbc(url=pg_url, table="d_cities", properties=pg_opts)
df_countries = spark.read.jdbc(url=pg_url, table="d_countries", properties=pg_opts)
df_prod_cats = spark.read.jdbc(url=pg_url, table="d_product_categories", properties=pg_opts)
df_products = spark.read.jdbc(url=pg_url, table="d_products", properties=pg_opts)
df_sales = spark.read.jdbc(url=pg_url, table="f_sales", properties=pg_opts)
df_customers = spark.read.jdbc(url=pg_url, table="d_customers", properties=pg_opts)
df_stores = spark.read.jdbc(url=pg_url, table="d_stores", properties=pg_opts)
df_suppliers = spark.read.jdbc(url=pg_url, table="d_suppliers", properties=pg_opts)

def mart_products():
    top = df_sales.groupBy("product_id").agg(_sum("product_quantity").alias("quantity_sold")) \
        .orderBy(desc("quantity_sold")).limit(10) \
        .join(df_products, df_sales.product_id == df_products.id, "left") \
        .select(df_sales.product_id.alias("id"), df_products.name, col("quantity_sold"))
    top.write.jdbc(url=ch_url, table="mart_products_top", mode="append", properties=ch_opts)

    revenue = df_sales.join(df_products, df_sales.product_id == df_products.id, "inner") \
        .join(df_prod_cats, df_products.category_id == df_prod_cats.id) \
        .groupBy(df_products.category_id) \
        .agg(_sum("total_price").alias("revenue")) \
        .join(df_prod_cats, df_products.category_id == df_prod_cats.id) \
        .select(df_products.category_id, df_prod_cats.name.alias("category"), col("revenue")) \
        .orderBy(desc("revenue"))
    revenue.write.jdbc(url=ch_url, table="mart_products_revenue", mode="append", properties=ch_opts)

def mart_customers():
    top = df_sales.groupBy("customer_id").agg(_sum("total_price").alias("spent_total")) \
        .orderBy(desc("spent_total")).limit(10) \
        .join(df_customers, df_sales.customer_id == df_customers.id, "left") \
        .select(df_sales.customer_id.alias("id"), df_customers.first_name.alias("fname"), df_customers.last_name.alias("lname"), col("spent_total"))
    top.write.jdbc(url=ch_url, table="mart_customers_top", mode="append", properties=ch_opts)

    by_country = df_customers.groupBy("country_id").agg(_count("*").alias("customers_count")) \
        .join(df_countries, df_customers.country_id == df_countries.id, "left") \
        .select(df_customers.country_id, df_countries.name.alias("country"), col("customers_count"))
    by_country.write.jdbc(url=ch_url, table="mart_customers_by_country", mode="append", properties=ch_opts)

    avg_check = df_sales.groupBy("customer_id").agg(_avg("total_price").alias("avg_check")) \
        .join(df_customers, df_sales.customer_id == df_customers.id, "left") \
        .select(df_sales.customer_id.alias("id"), df_customers.first_name.alias("fname"), df_customers.last_name.alias("lname"), col("avg_check"))
    avg_check.write.jdbc(url=ch_url, table="mart_customers_avg_check", mode="append", properties=ch_opts)

def mart_times():
    sales_dates = df_sales.join(df_dates, df_sales.date_id == df_dates.id, "inner")
    yearly = sales_dates.groupBy(year("date").alias("year")).agg(_sum("total_price").alias("revenue"), _sum("product_quantity").alias("items_sold")).orderBy("year")
    yearly.write.jdbc(url=ch_url, table="mart_yearly_sales", mode="append", properties=ch_opts)

    monthly = sales_dates.groupBy(year("date").alias("year"), month("date").alias("month")) \
        .agg(_sum("total_price").alias("revenue"), _sum("product_quantity").alias("items_sold")) \
        .orderBy("year", "month")
    monthly.write.jdbc(url=ch_url, table="mart_monthly_sales", mode="append", properties=ch_opts)

    sales_periods = sales_dates.withColumn("year", year("date")).withColumn("month", month("date")).withColumn("quarter", quarter("date"))
    monthly_rev = sales_periods.groupBy("year", "month").agg(_sum("total_price").alias("revenue")).orderBy("year", "month")
    w = Window.orderBy("year", "month")
    monthly_cmp = monthly_rev.withColumn("prev_revenue", lag("revenue", 1).over(w)) \
        .withColumn("growth", (col("revenue") - col("prev_revenue")) / col("prev_revenue") * 100).na.fill(0)
    monthly_cmp.write.jdbc(url=ch_url, table="mart_monthly_comparison", mode="append", properties=ch_opts)

    quarterly = sales_periods.groupBy("year", "quarter").agg(_sum("total_price").alias("revenue")).orderBy("year", "quarter")
    quarterly.write.jdbc(url=ch_url, table="mart_quarterly_sales", mode="append", properties=ch_opts)

    avg_order = sales_periods.groupBy("year", "month").agg(_avg("total_price").alias("avg_value"), _avg("product_quantity").alias("avg_items"), _count("*").alias("orders")).orderBy("year", "month")
    avg_order.write.jdbc(url=ch_url, table="mart_avg_order", mode="append", properties=ch_opts)

def mart_stores():
    top = df_sales.groupBy("store_id").agg(_sum("total_price").alias("revenue")) \
        .orderBy(desc("revenue")).limit(5) \
        .join(df_stores, df_sales.store_id == df_stores.id, "left") \
        .select(df_sales.store_id.alias("id"), df_stores.name.alias("store"), col("revenue"))
    top.write.jdbc(url=ch_url, table="mart_stores_top", mode="append", properties=ch_opts)

    geo = df_sales.join(df_stores, df_sales.store_id == df_stores.id, "inner") \
        .join(df_cities, df_stores.city_id == df_cities.id, "inner") \
        .join(df_countries, df_stores.country_id == df_countries.id, "inner")
    by_country = geo.groupBy("country_id", df_countries.name.alias("country")).agg(_sum("total_price").alias("revenue"), _sum("product_quantity").alias("items_sold"), _count(df_sales.id).alias("orders")).orderBy(desc("revenue"))
    by_country.write.jdbc(url=ch_url, table="mart_sales_by_country", mode="append", properties=ch_opts)
    by_city = geo.groupBy("city_id", df_cities.name.alias("city"), "country_id", df_countries.name.alias("country")).agg(_sum("total_price").alias("revenue"), _sum("product_quantity").alias("items_sold"), _count(df_sales.id).alias("orders")).orderBy(desc("revenue"))
    by_city.write.jdbc(url=ch_url, table="mart_sales_by_city", mode="append", properties=ch_opts)
    avg_receipt = df_sales.join(df_stores, df_sales.store_id == df_stores.id, "inner") \
        .groupBy("store_id", df_stores.name.alias("store"), df_stores.city_id.alias("city_id")) \
        .agg(_avg("total_price").alias("avg_receipt")).orderBy(desc("avg_receipt"))
    avg_receipt.write.jdbc(url=ch_url, table="mart_avg_receipt", mode="append", properties=ch_opts)

def mart_suppliers():
    top = df_sales.groupBy("supplier_id").agg(_sum("total_price").alias("revenue")) \
        .orderBy(desc("revenue")).limit(5) \
        .join(df_suppliers, df_sales.supplier_id == df_suppliers.id, "left") \
        .select(df_sales.supplier_id.alias("id"), df_suppliers.name.alias("supplier"), col("revenue"))
    top.write.jdbc(url=ch_url, table="mart_suppliers_top", mode="append", properties=ch_opts)

    with_price = df_sales.withColumn("item_price", col("total_price") / col("product_quantity"))
    avg_price = with_price.join(df_suppliers, with_price.supplier_id == df_suppliers.id, "inner") \
        .groupBy("supplier_id", df_suppliers.name.alias("supplier")).agg(_avg("item_price").alias("avg_price")).orderBy(desc("avg_price"))
    avg_price.write.jdbc(url=ch_url, table="mart_suppliers_avg_price", mode="append", properties=ch_opts)

    by_country = df_sales.join(df_suppliers, df_sales.supplier_id == df_suppliers.id, "inner") \
        .join(df_countries, df_suppliers.country_id == df_countries.id, "inner")
    dist = by_country.groupBy("country_id", df_countries.name.alias("country")).agg(_sum("total_price").alias("revenue"), _sum("product_quantity").alias("items_sold"), _count(df_sales.id).alias("orders"), countDistinct("supplier_id").alias("suppliers")).orderBy(desc("revenue"))
    dist.write.jdbc(url=ch_url, table="mart_suppliers_distribution", mode="append", properties=ch_opts)

def mart_quality():
    best = df_products.orderBy(desc("rating")).limit(100).select(col("id"), col("name"), col("rating"))
    best.write.jdbc(url=ch_url, table="mart_products_best", mode="append", properties=ch_opts)
    worst = df_products.orderBy(asc("rating")).limit(100).select(col("id"), col("name"), col("rating"))
    worst.write.jdbc(url=ch_url, table="mart_products_worst", mode="append", properties=ch_opts)
    sales_by_prod = df_sales.groupBy("product_id").agg(_sum("product_quantity").alias("quantity_sold"), _sum("total_price").alias("revenue"))
    stats = sales_by_prod.join(df_products, sales_by_prod.product_id == df_products.id, "inner").filter(col("rating").isNotNull())
    assembler = VectorAssembler(inputCols=["rating", "quantity_sold", "revenue"], outputCol="features")
    features = assembler.transform(stats)
    corr_mtx = Correlation.corr(features, "features").collect()[0][0]
    corr_rating_qty = corr_mtx.toArray()[0][1]
    corr_rating_rev = corr_mtx.toArray()[0][2]
    sql_corr = stats.select(corr("rating", "quantity_sold").alias("corr_rating_qty"), corr("rating", "revenue").alias("corr_rating_rev")).collect()[0]
    report = spark.createDataFrame([
        ("Rating vs Quantity", float(corr_rating_qty), float(sql_corr["corr_rating_qty"])),
        ("Rating vs Revenue", float(corr_rating_rev), float(sql_corr["corr_rating_rev"]))
    ], ["metric", "corr_matrix", "corr_sql"])
    report.write.jdbc(url=ch_url, table="mart_quality_correlation", mode="append", properties=ch_opts)
    reviews = df_products.orderBy(desc("reviews")).limit(100).select(col("id"), col("name"), col("reviews"))
    reviews.write.jdbc(url=ch_url, table="mart_products_reviews", mode="append", properties=ch_opts)

mart_products()
mart_customers()
mart_times()
mart_stores()
mart_suppliers()
mart_quality()

print("sleeping...")
sleep(2)
print("woke up!")

spark.stop()
