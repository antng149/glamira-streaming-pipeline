import os
import psycopg2
from psycopg2.extras import execute_values
import sys
import traceback
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, to_timestamp, date_format, year, month, dayofmonth, hour
from pyspark.sql.types import (
    StringType, StructType, StructField,
    LongType, ArrayType, MapType, BooleanType
)
from user_agents import parse as ua_parse
from datetime import datetime

# ─────────────────────────────────────────
# POSTGRES CONFIG
# ─────────────────────────────────────────
PG_CONN = {
    "host":     os.getenv("PG_HOST"),      
    "port":     int(os.getenv("PG_PORT", "5432")),  
    "dbname":   os.getenv("PG_DB"),        
    "user":     os.getenv("PG_USER"),      
    "password": os.getenv("PG_PASSWORD")   
}

# ─────────────────────────────────────────
# KAFKA CONFIG
# ─────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
KAFKA_SASL_MECHANISM    = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "product_view")

KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD")

KAFKA_SASL_JAAS_CONFIG = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{KAFKA_SASL_USERNAME}" password="{KAFKA_SASL_PASSWORD}";'
)

if not KAFKA_SASL_USERNAME or not KAFKA_SASL_PASSWORD:
    raise ValueError("Kafka username/password env vars are missing")

# ─────────────────────────────────────────
# CHECKPOINT CONFIG
# ─────────────────────────────────────────
_run_mode = os.getenv("SPARK_RUN_MODE", "local")
CHECKPOINT_LOCATION = (
    "hdfs://namenode/checkpoint/glamira-yarn"
    if _run_mode == "yarn"
    else "hdfs://namenode/checkpoint/glamira-local"
)


# ─────────────────────────────────────────
# KAFKA SCHEMA
# ─────────────────────────────────────────
glamira_schema = StructType([
    StructField("id",                  StringType(),  True),
    StructField("time_stamp",          LongType(),    True),
    StructField("ip",                  StringType(),  True),
    StructField("user_agent",          StringType(),  True),
    StructField("resolution",          StringType(),  True),
    StructField("device_id",           StringType(),  True),
    StructField("api_version",         StringType(),  True),
    StructField("store_id",            StringType(),  True),
    StructField("local_time",          StringType(),  True),
    StructField("show_recommendation", BooleanType(), True),
    StructField("current_url",         StringType(),  True),
    StructField("referrer_url",        StringType(),  True),
    StructField("email_address",       StringType(),  True),
    StructField("collection",          StringType(),  True),
    StructField("product_id",          StringType(),  True),
    StructField("option", ArrayType(
        MapType(StringType(), StringType())
    ), True),
])

# ─────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────
def get_browser(ua):
    try:
        return ua_parse(ua).browser.family
    except:
        return "Unknown"

def get_os(ua):
    try:
        return ua_parse(ua).os.family
    except:
        return "Unknown"

def get_device_type(ua):
    try:
        parsed = ua_parse(ua)
        if parsed.is_mobile:
            return "Mobile"
        elif parsed.is_tablet:
            return "Tablet"
        else:
            return "Desktop"
    except:
        return "Unknown"

def extract_domain(url):
    try:
        if not url:
            return None
        parsed = urlparse(url)
        return parsed.netloc.replace("www.", "")
    except:
        return None

def extract_country_from_domain(domain):
    # glamira.fr → fr, glamira.co.uk → uk, glamira.com → com
    try:
        if not domain:
            return None
        parts = domain.split(".")
        if len(parts) >= 2:
            return parts[-1].upper()
        return None
    except:
        return None

# ─────────────────────────────────────────
# UDFs
# ─────────────────────────────────────────
browser_udf      = udf(get_browser,     StringType())
os_udf           = udf(get_os,          StringType())
device_type_udf  = udf(get_device_type, StringType())
domain_udf       = udf(extract_domain,  StringType())

# ─────────────────────────────────────────
# DIM DEVICE
# ─────────────────────────────────────────
def process_dim_device(df):
    return df.select(
        browser_udf(col("user_agent")).alias("browser"),
        os_udf(col("user_agent")).alias("os"),
        device_type_udf(col("user_agent")).alias("device_type")
    ).distinct()


def store_dim_device(df):
    def write_partition(rows):
        conn = psycopg2.connect(**PG_CONN)
        cursor = conn.cursor()
        try:
            data = [(row["browser"], row["os"], row["device_type"]) for row in rows]
            if data:
                execute_values(cursor, """
                    INSERT INTO dim_device (browser, os, device_type)
                    VALUES %s
                    ON CONFLICT (browser, os, device_type) DO NOTHING
                """, data)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
    df.foreachPartition(write_partition)

# ─────────────────────────────────────────
# DIM DATE
# ─────────────────────────────────────────
def process_dim_date(df):
    event_ts = to_timestamp(col("local_time"), "yyyy-MM-dd HH:mm:ss")
    return df.select(
        date_format(event_ts, "yyyyMMddHH").cast("int").alias("date_key"),
        year(event_ts).alias("year"),
        month(event_ts).alias("month"),
        dayofmonth(event_ts).alias("day"),
        hour(event_ts).alias("hour"),
    ).filter(col("date_key").isNotNull()).distinct()

def store_dim_date(df):
    def write_partition(rows):
        conn = psycopg2.connect(**PG_CONN)
        cursor = conn.cursor()
        try:
            data = [(row["date_key"], row["year"], row["month"], row["day"], row["hour"]) for row in rows]
            if data:
                execute_values(cursor, """
                    INSERT INTO dim_date (date_key, year, month, day, hour)
                    VALUES %s
                    ON CONFLICT (date_key) DO NOTHING
                """, data)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
    df.foreachPartition(write_partition)

# ─────────────────────────────────────────
# DIM PRODUCT
# ─────────────────────────────────────────
def process_dim_product(df):
    return df.select("product_id") \
             .filter(col("product_id").isNotNull()) \
             .distinct()

def store_dim_product(df):
    def write_partition(rows):
        conn = psycopg2.connect(**PG_CONN)
        cursor = conn.cursor()
        try:
            data = [(row["product_id"],) for row in rows]
            if data:
                execute_values(cursor, """
                    INSERT INTO dim_product (product_id)
                    VALUES %s
                    ON CONFLICT (product_id) DO NOTHING
                """, data)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
    df.foreachPartition(write_partition)

# ─────────────────────────────────────────
# DIM STORE
# ─────────────────────────────────────────
def process_dim_store(df):
    return df.select("store_id") \
             .filter(col("store_id").isNotNull()) \
             .distinct()

def store_dim_store(df):
    def write_partition(rows):
        conn = psycopg2.connect(**PG_CONN)
        cursor = conn.cursor()
        try:
            data = [(row["store_id"],) for row in rows]
            if data:
                execute_values(cursor, """
                    INSERT INTO dim_store (store_id)
                    VALUES %s
                    ON CONFLICT (store_id) DO NOTHING
                """, data)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
    df.foreachPartition(write_partition)

# ─────────────────────────────────────────
# DIM REFERRER
# ─────────────────────────────────────────
def process_dim_referrer(df):
    return df.select(
        domain_udf(col("referrer_url")).alias("referrer_domain")
    ).filter(col("referrer_domain").isNotNull()) \
     .distinct()

def store_dim_referrer(df):
    def write_partition(rows):
        conn = psycopg2.connect(**PG_CONN)
        cursor = conn.cursor()
        try:
            data = [(row["referrer_domain"],) for row in rows]
            if data:
                execute_values(cursor, """
                    INSERT INTO dim_referrer (referrer_domain)
                    VALUES %s
                    ON CONFLICT (referrer_domain) DO NOTHING
                """, data)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
    df.foreachPartition(write_partition)

# ─────────────────────────────────────────
# DIM LOCATION
# ─────────────────────────────────────────
def process_dim_location(df):
    return df.select(
        col("ip").alias("ip_address"),
        domain_udf(col("current_url")).alias("domain")
    ).filter(col("ip_address").isNotNull()) \
     .distinct()

def store_dim_location(df):
    def write_partition(rows):
        conn = psycopg2.connect(**PG_CONN)
        cursor = conn.cursor()
        try:
            data = [
                (row["ip_address"], extract_country_from_domain(row["domain"]), row["domain"])
                for row in rows
            ]
            if data:
                execute_values(cursor, """
                    INSERT INTO dim_location (ip_address, country_code, domain)
                    VALUES %s
                    ON CONFLICT (ip_address) DO NOTHING
                """, data)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
    df.foreachPartition(write_partition)

# ─────────────────────────────────────────
# FACT TABLE
# ─────────────────────────────────────────
def store_fact(df, spark):
    # read dim tables from Postgres into Spark DataFrames
    def read_dim(table):
        return spark.read \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", f"jdbc:postgresql://{PG_CONN['host']}:{PG_CONN['port']}/{PG_CONN['dbname']}") \
            .option("dbtable", table) \
            .option("user", PG_CONN["user"]) \
            .option("password", PG_CONN["password"]) \
            .load()

    # load dims
    dim_device   = read_dim("dim_device")
    dim_product  = read_dim("dim_product")
    dim_store    = read_dim("dim_store")
    dim_location = read_dim("dim_location")
    dim_referrer = read_dim("dim_referrer")
    dim_date     = read_dim("dim_date")

    # add parsed columns to df
    enriched = df \
        .withColumn("browser",      browser_udf(col("user_agent"))) \
        .withColumn("os",           os_udf(col("user_agent"))) \
        .withColumn("device_type",  device_type_udf(col("user_agent"))) \
        .withColumn("domain",       domain_udf(col("current_url"))) \
        .withColumn("referrer_domain", domain_udf(col("referrer_url"))) \
        .withColumn("event_ts",     to_timestamp(col("local_time"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("event_hour",   hour(col("event_ts"))) \
        .withColumn("date_key",     date_format(col("event_ts"), "yyyyMMddHH").cast("int"))

    # JOIN to get all foreign keys
    fact = enriched \
        .join(dim_device,
              (enriched.browser     == dim_device.browser) &
              (enriched.os          == dim_device.os) &
              (enriched.device_type == dim_device.device_type),
              "left") \
        .join(dim_product,
              enriched.product_id == dim_product.product_id,
              "left") \
        .join(dim_store,
              enriched.store_id == dim_store.store_id,
              "left") \
        .join(dim_location,
              enriched.ip == dim_location.ip_address,
              "left") \
        .join(dim_referrer,
              enriched.referrer_domain == dim_referrer.referrer_domain,
              "left") \
        .select(
            enriched.id.alias("view_id"),
            enriched.date_key,
            dim_product.product_key,
            dim_store.store_key,
            dim_location.location_key,
            dim_device.device_key,
            dim_referrer.referrer_key,
            enriched.event_ts,
            enriched.event_hour,
            enriched.time_stamp,
            enriched.current_url,
            enriched.referrer_url,
            enriched.ip.alias("ip_address"),
            enriched.collection,
            enriched.api_version
        )

    # write using foreachPartition
    def write_partition(rows):
        conn = psycopg2.connect(**PG_CONN)
        cursor = conn.cursor()
        try:
            ingested_at = datetime.utcnow()
            data = [
                (
                    row["view_id"], row["date_key"], row["product_key"], row["store_key"],
                    row["location_key"], row["device_key"], row["referrer_key"],
                    row["event_ts"], row["event_hour"], row["time_stamp"],
                    row["current_url"], row["referrer_url"], row["ip_address"],
                    row["collection"], row["api_version"], ingested_at
                )
                for row in rows
            ]
            if data:
                execute_values(cursor, """
                    INSERT INTO fact_product_view (
                        view_id, date_key, product_key, store_key,
                        location_key, device_key, referrer_key,
                        event_ts, event_hour, time_stamp,
                        current_url, referrer_url, ip_address,
                        collection, api_version, ingested_at
                    )
                    VALUES %s
                    ON CONFLICT (view_id) DO NOTHING
                """, data)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    fact.foreachPartition(write_partition)

# ─────────────────────────────────────────
# FOREACH BATCH
# ─────────────────────────────────────────

def store_bad_records(df, batch_id):
    bad_df = df.select(
        col("raw_value").alias("kafka_value")
    )

    def write_partition(rows):
        conn = psycopg2.connect(**PG_CONN)
        cursor = conn.cursor()
        try:
            ingested_at = datetime.utcnow()
            data = [
                (batch_id, row["kafka_value"], "MALFORMED_JSON_OR_SCHEMA_MISMATCH", ingested_at)
                for row in rows
            ]
            if data:
                execute_values(cursor, """
                    INSERT INTO bad_product_view_records (
                        batch_id, kafka_value, error_reason, ingested_at
                    )
                    VALUES %s
                """, data)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    bad_df.foreachPartition(write_partition)

def process_batch(df, batch_id):
    print(f"Processing batch {batch_id}", flush=True)
    try:
        raw_parsed_df = df.select(
            col("value").cast(StringType()).alias("raw_value"),
            from_json(
                col("value").cast(StringType()),
                glamira_schema
            ).alias("data")
        )

        bad_records_df = raw_parsed_df.filter(col("data").isNull())

        parsed_df = raw_parsed_df \
            .filter(col("data").isNotNull()) \
            .select("data.*")

        parsed_df.cache()
        bad_records_df.cache()

        bad_record_count = bad_records_df.count()
        valid_record_count = "not_counted"

        print(
            f"Processing parsed batch {batch_id}: "
            f"valid_records={valid_record_count}, bad_records={bad_record_count}",
            flush=True,
        )

        if bad_record_count > 0:
            print(
                f"WARNING: batch {batch_id} contains {bad_record_count} malformed records",
                flush=True,
            )

        store_bad_records(bad_records_df, batch_id)

        store_dim_device(process_dim_device(parsed_df))
        store_dim_date(process_dim_date(parsed_df))
        store_dim_product(process_dim_product(parsed_df))
        store_dim_store(process_dim_store(parsed_df))
        store_dim_referrer(process_dim_referrer(parsed_df))
        store_dim_location(process_dim_location(parsed_df))
        store_fact(parsed_df, df.sparkSession)

        parsed_df.unpersist()
        bad_records_df.unpersist()

        print(
            f"Batch {batch_id} done ✅ valid_records={valid_record_count}, "
            f"bad_records={bad_record_count}",
            flush=True,
        )
    except Exception as e:
        print(f"ERROR in batch {batch_id}: {str(e)}", flush=True) 
        traceback.print_exc()
        raise

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("GlamiraKafkaStreaming") \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",  KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe",                KAFKA_TOPIC) \
        .option("startingOffsets",          "earliest") \
        .option("maxOffsetsPerTrigger",      10000) \
        .option("kafka.security.protocol",  KAFKA_SECURITY_PROTOCOL) \
        .option("kafka.sasl.mechanism",     KAFKA_SASL_MECHANISM) \
        .option("kafka.sasl.jaas.config",   KAFKA_SASL_JAAS_CONFIG) \
        .load()


    query = kafka_df \
        .writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime="30 seconds") \
        .start()


    query.awaitTermination()
