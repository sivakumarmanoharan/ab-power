import dlt
import re
from pyspark.sql import functions as F 
from pyspark.sql.types import StructField, StructType, ArrayType, MapType, StringType

LANDING = "/Volumes/ab_power/landing/raw_files"
API_DATASETS = ["pool_price", "load", "generation", "assets"]

INVALID_CHARS = r"[ ,;{}()\n\t=]+"

def clean_name(name: str) -> str:
    """'Actual Forecast Report' -> 'actual_forecast_report'. Leading underscores are kept."""
    return re.sub(INVALID_CHARS, "_", name.strip()).lower()

def clean_type(dt):
    """Recursively rename fields inside structs, arrays and maps."""
    if isinstance(dt, StructType):
        return StructType([StructField(clean_name(f.name), clean_type(f.dataType), f.nullable)
                           for f in dt.fields])
    if isinstance(dt, ArrayType):
        return ArrayType(clean_type(dt.elementType), dt.containsNull)
    if isinstance(dt, MapType):
        return MapType(dt.keyType, clean_type(dt.valueType), dt.valueContainsNull)
    return dt

def clean_columns(df):
    schema = df.schema
    return df.select([
        F.col(f"`{f.name}`").cast(clean_type(f.dataType)).alias(clean_name(f.name))
        for f in schema.fields
    ])

def add_ingestion_metadata(df):
    '''
    Addition of ingestion metadata in tables for bronze layer dlt tables
    Batch id is the timestamp extracted from the file path
    '''
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_batch_id", F.regexp_extract(F.col("_metadata.file_path"), r"_(\d{8}T\d{6}Z)\.json$",1 
            )
        )
    )

def create_bronze_table(dataset):
    @dlt.table(
        name = f"ab_power.bronze.{dataset}",
        comment = f"Bronze layer table for {dataset}",
        table_properties={"quality":"bronze"}
    )
    def bronze_table():
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format","json")
            .option("multiLine", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(f"{LANDING}/api/{dataset}/")
        )
        return add_ingestion_metadata(clean_columns(df))
    return bronze_table

for ds in API_DATASETS:
    create_bronze_table(ds)


# @dlt.table(
#     name = "ab_power.bronze.csd_generation_backfill",
#     comment="Historical CSD generation CSV files from AESO loaded as backfill",
#     table_properties={"quality":"bronze"}
# )
# def csd_generation_backfill():
#     df =(
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format","csv")
#         .option("header","true")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#         .load(f"{LANDING}/backfill/csd_generation")
#     )
#     return (
#         df.withColumn("_ingested_at", F.current_timestamp())
#         .withColumn("_source_file", F.col("_metadata.file_path"))
#         .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
#     )
