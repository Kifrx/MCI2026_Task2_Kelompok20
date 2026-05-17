from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.fpm import FPGrowth
from clickhouse_driver import Client
import os
import glob

def run_spark_analytics():
    print("1. Menginisialisai SPARK...")
    spark = SparkSession.builder \
        .appName("Orders_Analytics_Final") \
        .master("local[*]") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    try:
        print("2. Mmbaca parquet & flattening...")
        df_raw = spark.read.parquet("file:///opt/airflow/data_lake/order/")
        
        df_exploded = df_raw.withColumn("product_item", F.explode("products"))
        
        df_flat = df_exploded.select(
            F.col("order_id").cast("int").alias("order_id"),
            F.col("user_id").cast("int").alias("user_id"),
            F.col("order_number").cast("int").alias("order_number"),
            F.col("order_dow").cast("int").alias("order_dow"),
            F.col("order_hour_of_day").cast("int").alias("order_hour_of_day"),
            F.col("days_since_prior_order").cast("float").alias("days_since_prior_order"),
            F.col("product_item.product_id").cast("int").alias("product_id"),
            F.col("product_item.product_name").cast("string").alias("product_name"),
            F.col("product_item.department").cast("string").alias("department"),
            F.col("product_item.reordered").cast("int").alias("reordered")
        )

        print("3. Menjalankan FP-GROWTH...")
        df_basket = df_flat.groupBy("order_id").agg(
            F.collect_set("product_name").alias("items")
        )

        fpGrowth = FPGrowth(itemsCol="items", minSupport=0.02, minConfidence=0.1)
        model = fpGrowth.fit(df_basket)

        df_rules = model.associationRules
        df_rules_clean = df_rules.select(
            F.concat_ws(", ", F.col("antecedent")).alias("antecedent"),
            F.concat_ws(", ", F.col("consequent")).alias("consequent"),
            F.col("confidence").cast("float").alias("confidence"),
            F.col("lift").cast("float").alias("lift"),
            F.col("support").cast("float").alias("support")
        )

        print("4. Convert ke pandas...")
        final_results = df_flat.toPandas()
        rules_results = df_rules_clean.toPandas()
        
    except Exception as e:
        print(f"!!! ERROR SPARK: {e} !!!")
        raise e
    finally:
        spark.stop()
        print("5. SPARK done....")

    print("6. Memuat ke Clickhouse ...")
    client = Client(host='clickhouse-server', user='admin', password='rahasia')
    
    client.execute('CREATE DATABASE IF NOT EXISTS analytics')
    
    # Membuata tbel utama
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.order_items (
            order_id Int32,
            user_id Int32,
            order_number Int32,
            order_dow Int32,
            order_hour_of_day Int32,
            days_since_prior_order Float32,
            product_id Int32,
            product_name String,
            department String,
            reordered Int32
        ) ENGINE = MergeTree()
        ORDER BY (order_dow, order_hour_of_day, order_id)
    ''')
    
    # membuat Tbl Rules FP-Growth
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.fp_growth_rules (
            antecedent String,
            consequent String,
            confidence Float32,
            lift Float32,
            support Float32
        ) ENGINE = MergeTree()
        ORDER BY (confidence, lift)
    ''')

    # Truncate & Insert data ke clickHouse
    client.execute('TRUNCATE TABLE analytics.order_items')
    data_tuples = [tuple(x) for x in final_results.to_numpy()]
    if data_tuples:
        client.execute('INSERT INTO analytics.order_items VALUES', data_tuples)
        
    client.execute('TRUNCATE TABLE analytics.fp_growth_rules')
    rules_tuples = [tuple(x) for x in rules_results.to_numpy()]
    if rules_tuples:
        client.execute('INSERT INTO analytics.fp_growth_rules VALUES', rules_tuples)

    print("7. Membersihkan file Parquet lama dari Data Lake...")
    for f in glob.glob('/opt/airflow/data_lake/order/*.parquet'):
        try: os.remove(f)
        except OSError as e: print(f"Error: {f} : {e.strerror}")
    
    print("✅ PIPELINE SELESAI")

if __name__ == "__main__":
    run_spark_analytics()