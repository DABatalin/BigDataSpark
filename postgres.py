from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, monotonically_increasing_id
from time import sleep

def build_postgres_tables(session, df, jdbc_url, opts):
    # страны
    country_df = (df.select(col("customer_country").alias("country"))
                    .union(df.select(col("seller_country").alias("country")))
                    .union(df.select(col("store_country").alias("country")))
                    .union(df.select(col("supplier_country").alias("country")))
                    .distinct().filter(col("country").isNotNull())
                    .withColumn("id", monotonically_increasing_id() + 100))
    country_df.write.jdbc(url=jdbc_url, table="d_countries", mode="overwrite", properties=opts)
    country_df = session.read.jdbc(url=jdbc_url, table="d_countries", properties=opts)

    # города
    city_df = (df.select(col("store_city").alias("city"))
                 .union(df.select(col("supplier_city").alias("city")))
                 .distinct().filter(col("city").isNotNull())
                 .withColumn("id", monotonically_increasing_id() + 100))
    city_df.write.jdbc(url=jdbc_url, table="d_cities", mode="overwrite", properties=opts)
    city_df = session.read.jdbc(url=jdbc_url, table="d_cities", properties=opts)

    # даты
    date_df = (df.select(to_date(col("sale_date"), "M/d/y").alias("date"))
                 .union(df.select(to_date(col("product_release_date"), "M/d/y").alias("date")))
                 .union(df.select(to_date(col("product_expiry_date"), "M/d/y").alias("date")))
                 .distinct().filter(col("date").isNotNull())
                 .withColumn("id", monotonically_increasing_id() + 100))
    date_df.write.jdbc(url=jdbc_url, table="d_dates", mode="overwrite", properties=opts)
    date_df = session.read.jdbc(url=jdbc_url, table="d_dates", properties=opts)

    # типы питомцев
    pet_type_df = (df.select(col("customer_pet_type").alias("type"))
                     .distinct().filter(col("type").isNotNull())
                     .withColumn("id", monotonically_increasing_id() + 100))
    pet_type_df.write.jdbc(url=jdbc_url, table="d_pet_types", mode="overwrite", properties=opts)
    pet_type_df = session.read.jdbc(url=jdbc_url, table="d_pet_types", properties=opts)

    # породы питомцев
    pet_breed_df = (df.select(col("customer_pet_breed").alias("breed"))
                      .distinct().filter(col("breed").isNotNull())
                      .withColumn("id", monotonically_increasing_id() + 100))
    pet_breed_df.write.jdbc(url=jdbc_url, table="d_pet_breeds", mode="overwrite", properties=opts)
    pet_breed_df = session.read.jdbc(url=jdbc_url, table="d_pet_breeds", properties=opts)

    # категории питомцев
    pet_cat_df = (df.select(col("pet_category").alias("category"))
                    .distinct().filter(col("category").isNotNull())
                    .withColumn("id", monotonically_increasing_id() + 100))
    pet_cat_df.write.jdbc(url=jdbc_url, table="d_pet_categories", mode="overwrite", properties=opts)
    pet_cat_df = session.read.jdbc(url=jdbc_url, table="d_pet_categories", properties=opts)

    # питомцы
    pets_df = (df.join(pet_type_df, df.customer_pet_type == pet_type_df.type, "left")
                 .join(pet_breed_df, df.customer_pet_breed == pet_breed_df.breed, "left")
                 .join(pet_cat_df, df.pet_category == pet_cat_df.category, "left")
                 .select(col("pet_type_df.id").alias("type_id"),
                         col("customer_pet_name").alias("name"),
                         col("pet_breed_df.id").alias("breed_id"),
                         col("pet_cat_df.id").alias("category_id"))
                 .distinct().withColumn("id", monotonically_increasing_id() + 100))
    pets_df.write.jdbc(url=jdbc_url, table="d_pets", mode="overwrite", properties=opts)
    pets_df = session.read.jdbc(url=jdbc_url, table="d_pets", properties=opts)

    # категории продуктов
    prod_cat_df = (df.select(col("product_category").alias("category"))
                     .distinct().filter(col("category").isNotNull())
                     .withColumn("id", monotonically_increasing_id() + 100))
    prod_cat_df.write.jdbc(url=jdbc_url, table="d_product_categories", mode="overwrite", properties=opts)
    prod_cat_df = session.read.jdbc(url=jdbc_url, table="d_product_categories", properties=opts)

    # цвета продуктов
    prod_color_df = (df.select(col("product_color").alias("color"))
                       .distinct().filter(col("color").isNotNull())
                       .withColumn("id", monotonically_increasing_id() + 100))
    prod_color_df.write.jdbc(url=jdbc_url, table="d_product_colors", mode="overwrite", properties=opts)
    prod_color_df = session.read.jdbc(url=jdbc_url, table="d_product_colors", properties=opts)

    # размеры продуктов
    prod_size_df = (df.select(col("product_size").alias("size"))
                      .distinct().filter(col("size").isNotNull())
                      .withColumn("id", monotonically_increasing_id() + 100))
    prod_size_df.write.jdbc(url=jdbc_url, table="d_product_sizes", mode="overwrite", properties=opts)
    prod_size_df = session.read.jdbc(url=jdbc_url, table="d_product_sizes", properties=opts)

    # бренды продуктов
    prod_brand_df = (df.select(col("product_brand").alias("brand"))
                       .distinct().filter(col("brand").isNotNull())
                       .withColumn("id", monotonically_increasing_id() + 100))
    prod_brand_df.write.jdbc(url=jdbc_url, table="d_product_brands", mode="overwrite", properties=opts)
    prod_brand_df = session.read.jdbc(url=jdbc_url, table="d_product_brands", properties=opts)

    # материалы продуктов
    prod_mat_df = (df.select(col("product_material").alias("material"))
                     .distinct().filter(col("material").isNotNull())
                     .withColumn("id", monotonically_increasing_id() + 100))
    prod_mat_df.write.jdbc(url=jdbc_url, table="d_product_materials", mode="overwrite", properties=opts)
    prod_mat_df = session.read.jdbc(url=jdbc_url, table="d_product_materials", properties=opts)

    # продукты
    products_df = (df.join(prod_cat_df, df.product_category == prod_cat_df.category, "left")
                     .join(prod_color_df, df.product_color == prod_color_df.color, "left")
                     .join(prod_size_df, df.product_size == prod_size_df.size, "left")
                     .join(prod_brand_df, df.product_brand == prod_brand_df.brand, "left")
                     .join(prod_mat_df, df.product_material == prod_mat_df.material, "left")
                     .join(date_df.alias("rel"), to_date(df.product_release_date, "M/d/y") == col("rel.date"), "left")
                     .join(date_df.alias("exp"), to_date(df.product_expiry_date, "M/d/y") == col("exp.date"), "left")
                     .select(col("product_name").alias("name"),
                             col("prod_cat_df.id").alias("category_id"),
                             col("product_price").alias("price"),
                             col("product_weight").alias("weight"),
                             col("prod_color_df.id").alias("color_id"),
                             col("prod_size_df.id").alias("size_id"),
                             col("prod_brand_df.id").alias("brand_id"),
                             col("prod_mat_df.id").alias("material_id"),
                             col("product_description").alias("description"),
                             col("product_rating").alias("rating"),
                             col("product_reviews").alias("reviews"),
                             col("rel.id").alias("release_date_id"),
                             col("exp.id").alias("expiry_date_id"))
                     .distinct().withColumn("id", monotonically_increasing_id() + 100))
    products_df.write.jdbc(url=jdbc_url, table="d_products", mode="overwrite", properties=opts)
    products_df = session.read.jdbc(url=jdbc_url, table="d_products", properties=opts)

    # клиенты
    cust_df = (df.join(country_df, df.customer_country == country_df.country, "left")
                 .join(pet_type_df, df.customer_pet_type == pet_type_df.type, "left")
                 .join(pet_breed_df, df.customer_pet_breed == pet_breed_df.breed, "left")
                 .join(pet_cat_df, df.pet_category == pet_cat_df.category, "left")
                 .join(pets_df, (df.customer_pet_name == pets_df.name) &
                                (pet_type_df.id == pets_df.type_id) &
                                (pet_breed_df.id == pets_df.breed_id) &
                                (pet_cat_df.id == pets_df.category_id), "left")
                 .select(col("customer_first_name").alias("first_name"),
                         col("customer_last_name").alias("last_name"),
                         col("customer_age").alias("age"),
                         col("customer_email").alias("email"),
                         col("country_df.id").alias("country_id"),
                         col("customer_postal_code").alias("postal_code"),
                         col("pets_df.id").alias("pet_id"))
                 .distinct().withColumn("id", monotonically_increasing_id() + 100))
    cust_df.write.jdbc(url=jdbc_url, table="d_customers", mode="overwrite", properties=opts)
    cust_df = session.read.jdbc(url=jdbc_url, table="d_customers", properties=opts)

    # продавцы
    sellers_df = (df.join(country_df, df.seller_country == country_df.country, "left")
                    .select(col("seller_first_name").alias("first_name"),
                            col("seller_last_name").alias("last_name"),
                            col("seller_email").alias("email"),
                            col("country_df.id").alias("country_id"),
                            col("seller_postal_code").alias("postal_code"))
                    .distinct().withColumn("id", monotonically_increasing_id() + 100))
    sellers_df.write.jdbc(url=jdbc_url, table="d_sellers", mode="overwrite", properties=opts)
    sellers_df = session.read.jdbc(url=jdbc_url, table="d_sellers", properties=opts)

    # магазины
    stores_df = (df.join(country_df.alias("co"), df.store_country == col("co.country"), "left")
                   .join(city_df.alias("ci"), df.store_city == col("ci.city"), "left")
                   .select(col("store_name").alias("name"),
                           col("store_location").alias("location"),
                           col("ci.id").alias("city_id"),
                           col("store_state").alias("state"),
                           col("co.id").alias("country_id"),
                           col("store_phone").alias("phone"),
                           col("store_email").alias("email"))
                   .distinct().withColumn("id", monotonically_increasing_id() + 100))
    stores_df.write.jdbc(url=jdbc_url, table="d_stores", mode="overwrite", properties=opts)
    stores_df = session.read.jdbc(url=jdbc_url, table="d_stores", properties=opts)

    # поставщики
    suppliers_df = (df.join(country_df.alias("co"), df.supplier_country == col("co.country"), "left")
                      .join(city_df.alias("ci"), df.supplier_city == col("ci.city"), "left")
                      .select(col("supplier_name").alias("name"),
                              col("supplier_contact").alias("contact"),
                              col("supplier_email").alias("email"),
                              col("supplier_phone").alias("phone"),
                              col("supplier_address").alias("address"),
                              col("ci.id").alias("city_id"),
                              col("co.id").alias("country_id"))
                      .distinct().withColumn("id", monotonically_increasing_id() + 100))
    suppliers_df.write.jdbc(url=jdbc_url, table="d_suppliers", mode="overwrite", properties=opts)
    suppliers_df = session.read.jdbc(url=jdbc_url, table="d_suppliers", properties=opts)

    # продажи
    sales_df = (df.join(cust_df, df.customer_email == col("cust_df.email"), "left")
                  .join(sellers_df, df.seller_email == col("sellers_df.email"), "left")
                  .join(products_df, (df.product_name == col("products_df.name")) &
                                     (df.product_price == col("products_df.price")) &
                                     (df.product_weight == col("products_df.weight")) &
                                     (df.product_description == col("products_df.description")) &
                                     (df.product_rating == col("products_df.rating")) &
                                     (df.product_reviews == col("products_df.reviews")), "left")
                  .join(date_df, to_date(df.sale_date, "M/d/y") == col("date_df.date"), "left")
                  .join(stores_df, df.store_email == col("stores_df.email"), "left")
                  .join(suppliers_df, df.supplier_email == col("suppliers_df.email"), "left")
                  .select(col("cust_df.id").alias("customer_id"),
                          col("sellers_df.id").alias("seller_id"),
                          col("products_df.id").alias("product_id"),
                          col("product_quantity").alias("product_quantity"),
                          col("date_df.id").alias("date_id"),
                          col("sale_quantity").alias("quantity"),
                          col("sale_total_price").alias("total_price"),
                          col("stores_df.id").alias("store_id"),
                          col("suppliers_df.id").alias("supplier_id"))
                  .withColumn("id", monotonically_increasing_id() + 100))
    sales_df.write.jdbc(url=jdbc_url, table="f_sales", mode="overwrite", properties=opts)
    sales_df = session.read.jdbc(url=jdbc_url, table="f_sales", properties=opts)

def run():
    spark = SparkSession.builder \
        .appName("BD Spark Postgres") \
        .config("spark.jars", "postgresql-42.6.0.jar,clickhouse-jdbc-0.4.6.jar") \
        .getOrCreate()
    url = "jdbc:postgresql://localhost:5432/spark_db"
    props = {"user": "spark_user", "password": "spark_password", "driver": "org.postgresql.Driver"}
    src_table = "mock_data"
    df = spark.read.jdbc(url=url, table=src_table, properties=props)
    build_postgres_tables(spark, df, url, props)
    print("sleeping...")
    sleep(2)
    print("woke up!")
    spark.stop()

if __name__ == "__main__":
    run()
