import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import get_json_object

from pyspark.sql.functions import col, when, to_date, to_timestamp, trim, lower, date_format, lit,broadcast
from pyspark.sql.types import DecimalType
from awsglue.dynamicframe import DynamicFrame

# JOB INIT
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# PATHS
source_base_path = "s3://data-pipeline-mock-raw-dev/"
target_base_path = "s3://data-pipeline-mock-stage-dev/"

# NULL VALUES
null_variations = ['NULL', 'null', 'Null', 'N/A', 'NA', '', '  ', 'None', 'none']

# ================= UTILITY FUNCTIONS ================= #

def clean_nulls(df):
    return df.replace(null_variations, None)

def drop_required(df, required_cols):
    return df.dropna(subset=required_cols)

def clean_dates(df, date_cols):
    for c in date_cols:
        if c in df.columns:
            df = df.withColumn(c, to_date(col(c)))
    return df

def clean_timestamps(df, ts_cols):
    for c in ts_cols:
        if c in df.columns:
            df = df.withColumn(c, to_timestamp(col(c)))
    return df

def cast_decimals(df, cols, precision=12, scale=2):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DecimalType(precision, scale)))
    return df

def drop_negative_rows(df, cols):
    """
    Drop any row where any of the specified columns has a negative value.
    """
    for c in cols:
        if c in df.columns:
            df = df.filter(col(c) >= 0)
    return df


def trim_lower_strings(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, lower(trim(col(c))))
    return df

def add_date_keys(df, cols):
    """add date_key for date/timestamp cols"""
    for c in cols:
        if c in df.columns:
            df = df.withColumn(f"{c}_date_key", date_format(col(c), "yyyyMMdd"))
    return df

def df_to_dynamicframe(df, name):
    return DynamicFrame.fromDF(df, glueContext, name)

def extract_json_fields(df):
    """Extract key fields from JSON columns for easier querying"""


    # Example: Extracting 'payment_channel' and 'device_type' from 'transaction_metadata' JSON column
    # if "transaction_metadata" in df.columns:
    #     df = df.withColumn("payment_channel", col("transaction_metadata").getItem("payment_channel"))
    #     df = df.withColumn("device_type", col("transaction_metadata").getItem("device_type"))
    
    # Extract from gateway_response JSON
    if "gateway_response" in df.columns:
        df = df.withColumn("gateway_fee", 
                          get_json_object(col("gateway_response"), "$.gateway_fee").cast(DecimalType(10, 2)))
        df = df.withColumn("gateway_status", 
                          get_json_object(col("gateway_response"), "$.gateway_status"))
        df = df.withColumn("gateway_transaction_id", 
                          get_json_object(col("gateway_response"), "$.gateway_transaction_id"))
        df = df.withColumn("gateway_name", 
                          get_json_object(col("gateway_response"), "$.gateway_name"))
    
    # Extract from transaction_metadata JSON
    if "transaction_metadata" in df.columns:
        df = df.withColumn("risk_score", 
                          get_json_object(col("transaction_metadata"), "$.risk_score").cast("int"))
        df = df.withColumn("processing_time_ms", 
                          get_json_object(col("transaction_metadata"), "$.processing_time_ms").cast("int"))
        df = df.withColumn("payment_channel", 
                          get_json_object(col("transaction_metadata"), "$.payment_channel"))
        df = df.withColumn("device_type", 
                          get_json_object(col("transaction_metadata"), "$.device_type"))
        df = df.withColumn("payment_latitude", 
                          get_json_object(col("transaction_metadata"), "$.payment_latitude").cast(DecimalType(10, 8)))
        df=df.withColumn("payment_longitude",get_json_object(col("transaction_metadata"),"$.payment_longitude").cast(DecimalType(11,8)))
    
    return df
    

# ================= MERCHANTS ================= #

def process_merchants(table):
    print("Processing merchants...")

    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [f"{source_base_path}payments_db/{table}"], "recurse": True},
        format="csv",
        format_options={"withHeader": True, "separator": ","},
        transformation_ctx="merchants_source"
    )
    if dyf.count() == 0:
        print(f"⚠️ No data found for {table}, skipping transactions processing")
        return
    df = dyf.toDF()
    
    df = clean_nulls(df)
    df = df.dropDuplicates(["merchant_id"])

    df = drop_required(df, ["merchant_id", "tax_id"])

    string_cols = ["merchant_name","legal_entity_name","business_category",
                   "merchant_tier","onboarding_status","region","city",
                   "state","country","postal_code","timezone"]
    df = trim_lower_strings(df, string_cols)

    # DATE fields
    date_cols = ["onboarding_date","contract_start_date","contract_end_date","last_transaction_date"]
    df = clean_dates(df, date_cols)
    df = add_date_keys(df, date_cols)

    # Decimals
    decimal_cols = ["commission_rate","monthly_fee"]
    df = cast_decimals(df, decimal_cols, 10, 2)
    df = drop_negative_rows(df, decimal_cols)




    dyf_out = df_to_dynamicframe(df, "merchants_silver")
    glueContext.write_dynamic_frame.from_options(
        frame=dyf_out,
        connection_type="s3",
        connection_options={"path": f"{target_base_path}payments_db/merchants/", "partitionKeys": ["country"]},
        format="parquet"
    )

    print(" Merchants processed")

# ================= TRANSACTIONS ================= #

def process_transactions(table):
    print("Processing transactions...")

    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [f"{source_base_path}payments_db/{table}"], "recurse": True},
        format="csv",
        format_options={"withHeader": True, "separator": ","},
        transformation_ctx="transactions_source"
    )
    if dyf.count() == 0:
        print(f"⚠️ No data found for {table}, skipping transactions processing")
        return
    

    df = dyf.toDF()
    df = clean_nulls(df)
    df = df.dropDuplicates(["transaction_id", "transaction_timestamp"])


    # ✅ ADD THIS LINE
    df = extract_json_fields(df)
    
    # TIMESTAMP fields
    timestamp_cols = ["transaction_timestamp", "processed_timestamp"]  # ⚠️ FIX: Use correct column names
    df = clean_timestamps(df, timestamp_cols)

    # Drop required AFTER casting
    df = drop_required(df, ["transaction_id", "merchant_id", "transaction_timestamp"])  # ⚠️ FIX

    # Add date_key for partitioning only
    df = add_date_keys(df, timestamp_cols)

    # Clean strings - ✅ UPDATE THIS LIST
    string_cols = ["gateway_status", "gateway_transaction_id", "gateway_name", "payment_channel", "device_type"]
    df = trim_lower_strings(df, string_cols)

    # Money columns - ✅ UPDATE THIS LIST
    money_cols = ["gross_amount", "discount_amount", "tax_amount", 
                  "net_amount", "refund_amount", "gateway_fee"]
    df = cast_decimals(df, money_cols, 12, 2)
    df = drop_negative_rows(df, money_cols)

    # Join merchants
    merchant_ids = spark.read.parquet(f"{target_base_path}payments_db/merchants/") \
            .select("merchant_id").distinct()
    df = df.join(broadcast(merchant_ids), "merchant_id", "semi")
    
    # Derive year/month for optimized partitioning - ✅ FIX
    df = df.withColumn("year", date_format(col("transaction_timestamp"), "yyyy"))
    df = df.withColumn("month", date_format(col("transaction_timestamp"), "MM"))

    dyf_out = df_to_dynamicframe(df, "transactions_silver")
    glueContext.write_dynamic_frame.from_options(
        frame=dyf_out,
        connection_type="s3",
        connection_options={
            "path": f"{target_base_path}payments_db/transactions/",
            "partitionKeys": ["year","month"]
        },
        format="parquet"
    )

    print(" Transactions processed")

# ================= MAIN ================= #

def main():
    process_merchants("merchants")
    process_transactions("transactions")

if __name__ == "__main__":
    main()
    job.commit()



# data arrives late and is ditto to a row inside silver so we join previous 7 days and remove by row_number fxn

# can store hash of row inside silver. just take this hash column and join with incoming batch
# if match then remove or whatever
# truly_new = new_data.join(existing_hashes, "row_hash", "left_anti")

#  have a staging in silver and partition in it based on date
# then you have partitions for the batch data so no need to load everything from silver and just load for this parition
# whatever exists in silver and deduplicate!