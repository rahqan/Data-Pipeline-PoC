import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, BooleanType
from awsglue.dynamicframe import DynamicFrame


# ─── JOB INIT ────────────────────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'redshift_jdbc_url',
    'redshift_user',
    'redshift_password',
    'temp_dir',
    'redshift_iam_role'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ─── JDBC HELPER ──────────────────────────────────────────────────────────────



def execute_redshift_sql(statements: list):
    from py4j.java_gateway import java_import
    java_import(spark._jvm, "java.sql.DriverManager")
    
    conn = spark._jvm.DriverManager.getConnection(
        args['redshift_jdbc_url'],
        args['redshift_user'],
        args['redshift_password']
    )
    stmt = conn.createStatement()
    
    for sql in statements:
        sql = sql.strip()
        if sql:
            print(f"  ▶ {sql[:80]}...")
            stmt.execute(sql)
    
    stmt.close()
    conn.close()


# ─── SPARK → REDSHIFT STAGING WRITER ─────────────────────────────────────────

def write_staging(df, stg_table):
    """Overwrite a staging table in Redshift via S3 temp."""
    df.write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", args['redshift_jdbc_url']) \
        .option("dbtable", f"staging.{stg_table}") \
        .option("user", args['redshift_user']) \
        .option("password", args['redshift_password']) \
        .option("tempdir", args['temp_dir']) \
        .option("aws_iam_role", args['redshift_iam_role']) \
        .mode("append") \
        .save()


def read_redshift(query):
    """Read from Redshift via spark-redshift connector."""
    return spark.read \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", args['redshift_jdbc_url']) \
        .option("query", query) \
        .option("user", args['redshift_user']) \
        .option("password", args['redshift_password']) \
        .option("tempdir", args['temp_dir']) \
        .option("aws_iam_role", args['redshift_iam_role']) \
        .load()


def drop_cols(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.drop(c)
    return df


# ─── JSON EXTRACTION ──────────────────────────────────────────────────────────

def extract_json_fields(df):
    """
    Silver already extracted: gateway_fee, gateway_status, gateway_transaction_id,
    gateway_name, risk_score, processing_time_ms, payment_channel, device_type,
    payment_latitude, payment_longitude.
    Gold extracts the remainder for fact + junk dim joins.
    """
    if "gateway_response" in df.columns:
        df = df.withColumn(
            "gateway_retry_count",
            F.get_json_object(F.col("gateway_response"), "$.gateway_retry_count").cast(IntegerType())
        )
        df = df.withColumn(
            "gateway_timestamp",
            F.to_timestamp(F.get_json_object(F.col("gateway_response"), "$.gateway_timestamp"))
        )

    if "transaction_metadata" in df.columns:
        df = df.withColumn(
            "ip_address",
            F.get_json_object(F.col("transaction_metadata"), "$.ip_address")
        )
        df = df.withColumn(
            "card_type",
            F.lower(F.trim(F.get_json_object(F.col("transaction_metadata"), "$.card_type")))
        )
        df = df.withColumn(
            "card_brand_name",
            F.lower(F.trim(F.get_json_object(F.col("transaction_metadata"), "$.card_brand")))
        )
        df = df.withColumn(
            "user_agent",
            F.get_json_object(F.col("transaction_metadata"), "$.user_agent")
        )
        df = df.withColumn(
            "is_online_flag",
            F.coalesce(
                F.get_json_object(F.col("transaction_metadata"), "$.is_online").cast(BooleanType()),
                F.lit(False)
            )
        )
        df = df.withColumn(
            "is_mac",
            F.when(F.col("user_agent").isNull(), F.lit(None).cast(BooleanType()))
             .otherwise(F.col("user_agent").rlike(".*Mac.*"))
        )

    return df


# ─── DIM DATE ─────────────────────────────────────────────────────────────────

def process_dim_date():
    print("Processing dim_date...")

    df = spark.sql("""
        SELECT explode(
            sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)
        ) AS full_date
    """)

    df = df \
        .withColumn("date_key",
                    F.date_format(F.col("full_date"), "yyyyMMdd").cast(IntegerType())) \
        .withColumn("day_of_week",
                    F.dayofweek(F.col("full_date"))) \
        .withColumn("day_of_week_name",
                    F.date_format(F.col("full_date"), "EEEE")) \
        .withColumn("day_of_month",
                    F.dayofmonth(F.col("full_date"))) \
        .withColumn("day_of_year",
                    F.dayofyear(F.col("full_date"))) \
        .withColumn("week_of_year",
                    F.weekofyear(F.col("full_date"))) \
        .withColumn("month_number",
                    F.month(F.col("full_date"))) \
        .withColumn("month_name",
                    F.date_format(F.col("full_date"), "MMMM")) \
        .withColumn("quarter",
                    F.quarter(F.col("full_date"))) \
        .withColumn("year",
                    F.year(F.col("full_date"))) \
        .withColumn("is_weekend",
                    F.dayofweek(F.col("full_date")).isin([1, 7])) \
        .withColumn("is_holiday",
                    F.lit(False)) \
        .withColumn("fiscal_year",
                    F.when(F.month(F.col("full_date")) >= 4,
                           F.year(F.col("full_date")) + 1)
                     .otherwise(F.year(F.col("full_date")))) \
        .withColumn("fiscal_quarter",
                    F.when(F.month(F.col("full_date")).between(4, 6),  F.lit(1))
                     .when(F.month(F.col("full_date")).between(7, 9),  F.lit(2))
                     .when(F.month(F.col("full_date")).between(10, 12), F.lit(3))
                     .otherwise(F.lit(4)))

    write_staging(df, "stg_dim_date")

    execute_redshift_sql(["""
        INSERT INTO public.dim_date (
            date_key, full_date,
            day_of_week, day_of_week_name,
            day_of_month, day_of_year, week_of_year,
            month_number, month_name,
            quarter, year,
            is_weekend, is_holiday,
            fiscal_year, fiscal_quarter
        )
        SELECT
            date_key, full_date,
            day_of_week, day_of_week_name,
            day_of_month, day_of_year, week_of_year,
            month_number, month_name,
            quarter, year,
            is_weekend, is_holiday,
            fiscal_year, fiscal_quarter
        FROM staging.stg_dim_date s
        WHERE NOT EXISTS (
            SELECT 1 FROM public.dim_date d WHERE d.date_key = s.date_key
        )
    """])

    print("✓ dim_date done")


# ─── DIM MERCHANT (SCD2) ──────────────────────────────────────────────────────

def process_dim_merchant():
    print("Processing dim_merchant...")

    dyf = glueContext.create_dynamic_frame.from_catalog(
        database='silver_db',
        table_name='merchants',
        transformation_ctx="merchants_gold_source"
    )   

    if dyf.count() == 0:
        print("⚠️ No merchant data, skipping")
        return

    df = dyf.toDF()

    df = df.withColumnRenamed("op", "cdc_op")

    df = df \
        .withColumnRenamed("business_category", "business_category_code") \
        .withColumnRenamed("merchant_tier",      "merchant_tier_code") \
        .withColumnRenamed("onboarding_status",  "onboarding_status_code")

    df = df \
        .withColumn("valid_from", F.to_date(F.col("cdc_timestamp"))) \
        .withColumn("valid_to",   F.lit("9999-12-31").cast("date")) \
        .withColumn("is_active",  F.lit(True))
    
    df = drop_cols(df, [
    "onboarding_date",
    "contract_start_date",
    "contract_end_date",
    "#"
])


    df=df.withColumnRenamed("onboarding_date_date_key","onboarding_date")
    df=df.withColumnRenamed("contract_start_date_date_key","contract_start_date")

    df=df.withColumnRenamed("contract_end_date_date_key","contract_end_date")

    df = df \
        .withColumn("onboarding_date",
                    F.col("onboarding_date").cast(IntegerType())) \
        .withColumn("contract_start_date",
                    F.col("contract_start_date").cast(IntegerType())) \
        .withColumn("contract_end_date",
                    F.col("contract_end_date").cast(IntegerType()))

    dim_cols = [
        "cdc_op", "cdc_timestamp",
        "merchant_id", "merchant_name", "legal_entity_name", "tax_id",
        "business_category_code", "merchant_tier_code", "onboarding_status_code",
        "region", "city", "state", "country", "postal_code", "timezone",
        "commission_rate", "monthly_fee",
        "onboarding_date", "contract_start_date", "contract_end_date",
        "valid_from", "valid_to", "is_active",
        "created_at", "updated_at"
    ]
    df = df.select([c for c in dim_cols if c in df.columns])

    execute_redshift_sql(["TRUNCATE TABLE staging.stg_merchant"])
    write_staging(df, "stg_merchant")

    # SCD2: expire then insert
    execute_redshift_sql([
        """
        UPDATE public.dim_merchant dm
        SET
            valid_to   = s.valid_from - INTERVAL '1 day',
            is_active  = FALSE,
            updated_at = s.updated_at
        FROM staging.stg_merchant s
        WHERE dm.merchant_id = s.merchant_id
          AND s.cdc_op            = 'U'
          AND dm.is_active    = TRUE
        """,
        """
        INSERT INTO public.dim_merchant (
            merchant_id, merchant_name, legal_entity_name, tax_id,
            business_category_code, merchant_tier_code, onboarding_status_code,
            region, city, state, country, postal_code, timezone,
            commission_rate, monthly_fee,
            onboarding_date, contract_start_date, contract_end_date,
            valid_from, valid_to, is_active,
            created_at, updated_at
        )
        SELECT
            merchant_id, merchant_name, legal_entity_name, tax_id,
            business_category_code, merchant_tier_code, onboarding_status_code,
            region, city, state, country, postal_code, timezone,
            commission_rate, monthly_fee,
            onboarding_date, contract_start_date, contract_end_date,
            valid_from, valid_to, is_active,
            created_at, updated_at
        FROM staging.stg_merchant
        WHERE cdc_op IN ('I', 'U')
        """
    ])

    print("✓ dim_merchant done")


# ─── FACT TRANSACTIONS ────────────────────────────────────────────────────────

def process_transactions():
    print("Processing fact_transactions...")

    dyf = glueContext.create_dynamic_frame.from_catalog(
        database='silver_db',
        table_name='transactions',
        transformation_ctx="transactions_gold_source"
    )

    if dyf.count() == 0:
        print("⚠️ No transaction data, skipping")
        return

    df = dyf.toDF()

    # ── Extract remaining JSON ────────────────────────────────────────────────
    df = extract_json_fields(df)

    # ── Derived fields ────────────────────────────────────────────────────────
    df = df.withColumn("discount_applied",
                       (F.col("discount_amount") > 0))

    df = df.withColumn("gateway_date_key",
                       F.when(F.col("gateway_timestamp").isNotNull(),
                              F.date_format(F.col("gateway_timestamp"), "yyyyMMdd").cast(IntegerType()))
                        .otherwise(F.lit(None).cast(IntegerType())))

    # Cast silver string date keys → int
    df = df \
        .withColumn("transaction_date_key",
                    F.col("transaction_timestamp_date_key").cast(IntegerType())) \
        .withColumn("processed_date_key",
                    F.col("processed_timestamp_date_key").cast(IntegerType()))

    df = df \
        .withColumn("year",  F.col("year").cast(IntegerType())) \
        .withColumn("month", F.col("month").cast(IntegerType()))

    df = df.withColumn("emi_enabled", F.col("emi_enabled").cast(BooleanType()))

    # Rename int cols to match junk dim join keys
    df = df \
        .withColumnRenamed("payment_method",     "payment_method_code") \
        .withColumnRenamed("transaction_status", "transaction_status_code") \
        .withColumnRenamed("currency",           "currency_code") \
        .withColumnRenamed("device_type",        "device_type_desc")

    # ── Lookup merchant_key (inner — drops transactions with unknown merchant) ─
    merchant_lookup = read_redshift("""
        SELECT merchant_id, merchant_key
        FROM public.dim_merchant
        WHERE is_active = TRUE
    """)
    df = df.join(F.broadcast(merchant_lookup), on="merchant_id", how="inner")

    # ── Lookup payment_junk_key ───────────────────────────────────────────────
    # IMPORTANT: gateway_name is lowercased by silver (stripe / razorpay / auropay)
    # Ensure junk dim was populated with lowercase gateway_name values.
    payment_junk = read_redshift("""
        SELECT payment_junk_key,
               payment_method_code,
               transaction_status_code,
               currency_code,
               gateway_name,
               is_online_flag
        FROM public.junk_dim_payment_context
    """)
    # df = df.join(
    #     F.broadcast(payment_junk),
    #     on=["payment_method_code", "transaction_status_code",
    #         "currency_code", "gateway_name","is_online_flag"],
    #     how="left"
    # )
    df = df.join(
        F.broadcast(payment_junk),
        on=[
            df["payment_method_code"].eqNullSafe(payment_junk["payment_method_code"]),
            df["transaction_status_code"].eqNullSafe(payment_junk["transaction_status_code"]),
            df["currency_code"].eqNullSafe(payment_junk["currency_code"]),
            df["gateway_name"].eqNullSafe(payment_junk["gateway_name"]),
            df["is_online_flag"].eqNullSafe(payment_junk["is_online_flag"])
        ],
        how="left"
    )
    df = df.withColumn("payment_junk_key",
                       F.coalesce(F.col("payment_junk_key"), F.lit(-1)))

    # ── Lookup card_device_junk_key ───────────────────────────────────────────
    card_junk = read_redshift("""
        SELECT card_device_junk_key,
               card_type,
               card_brand_name,
               device_type_desc,
               is_mac
        FROM public.junk_dim_card_device
    """)
    # df = df.join(
    #     F.broadcast(card_junk),
    #     on=["card_type", "card_brand_name", "device_type_desc","is_mac"],
    #     how="left"
    # )
    df = df.join(
        F.broadcast(card_junk),
        on=[
            df["card_type"].eqNullSafe(card_junk["card_type"]),
            df["card_brand_name"].eqNullSafe(card_junk["card_brand_name"]),
            df["device_type_desc"].eqNullSafe(card_junk["device_type_desc"]),
            df["is_mac"].eqNullSafe(card_junk["is_mac"])
        ],
        how="left"
    )
    df = df.withColumn("card_device_junk_key",
                       F.coalesce(F.col("card_device_junk_key"), F.lit(-1)))

    # ── Final column select ───────────────────────────────────────────────────
    fact_cols = [
        "transaction_id", "merchant_key", "customer_id",
        "payment_junk_key", "card_device_junk_key",
        "transaction_timestamp",  "transaction_date_key",
        "processed_timestamp",    "processed_date_key",
        "gateway_timestamp",      "gateway_date_key",
        "gross_amount", "discount_amount", "tax_amount",
        "net_amount",   "refund_amount",
        "emi_enabled", "emi_tenure_months", "emi_interest_rate",
        "ip_address", "payment_latitude", "payment_longitude",
        "processing_time_ms", "gateway_fee",
        "gateway_status", "gateway_retry_count",
        "discount_applied",
        "created_at",
        "year", "month"
    ]
    df = df.select([c for c in fact_cols if c in df.columns])
    execute_redshift_sql(["TRUNCATE TABLE staging.stg_fact_transactions"])

    write_staging(df, "stg_fact_transactions")

    # ── staging → fact (dedup guard on transaction_id) ────────────────────────
    execute_redshift_sql(["""
        INSERT INTO public.fact_transactions (
            transaction_id, merchant_key, customer_id,
            payment_junk_key, card_device_junk_key,
            transaction_timestamp,  transaction_date_key,
            processed_timestamp,    processed_date_key,
            gateway_timestamp,      gateway_date_key,
            gross_amount, discount_amount, tax_amount, net_amount, refund_amount,
            emi_enabled, emi_tenure_months, emi_interest_rate,
            ip_address, payment_latitude, payment_longitude,
            processing_time_ms, gateway_fee,
            gateway_status, gateway_retry_count,
            discount_applied,
            created_at,
            year, month
        )
        SELECT
            s.transaction_id, s.merchant_key, s.customer_id,
            s.payment_junk_key, s.card_device_junk_key,
            s.transaction_timestamp,  s.transaction_date_key,
            s.processed_timestamp,    s.processed_date_key,
            s.gateway_timestamp,      s.gateway_date_key,
            s.gross_amount, s.discount_amount, s.tax_amount, s.net_amount, s.refund_amount,
            s.emi_enabled, s.emi_tenure_months, s.emi_interest_rate,
            s.ip_address, s.payment_latitude, s.payment_longitude,
            s.processing_time_ms, s.gateway_fee,
            s.gateway_status, s.gateway_retry_count,
            s.discount_applied,
            s.created_at,
            s.year, s.month
        FROM staging.stg_fact_transactions s
        WHERE NOT EXISTS (
            SELECT 1 FROM public.fact_transactions f
            WHERE f.transaction_id = s.transaction_id
        )
    """])

    print("✓ fact_transactions done")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    process_dim_date()
    process_dim_merchant()
    process_transactions()


if __name__ == "__main__":
    main()
    job.commit()