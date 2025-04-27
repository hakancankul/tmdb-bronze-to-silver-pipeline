import findspark
findspark.init("/opt/spark")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import explode_outer, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from delta.tables import *
import os

# ======================= CONFIG =======================
ACCESS_KEY = os.getenv("ACCESS_KEY", "dataops")
SECRET_KEY = os.getenv("SECRET_KEY", "Ankara06")
ENDPOINT = os.getenv("ENDPOINT", "http://minio:9000")

BRONZE_PATH = "s3a://tmdb-bronze/credits/"
SILVER_CAST_PATH = "s3a://tmdb-silver/cast"
SILVER_CREW_PATH = "s3a://tmdb-silver/crew"

# ======================= FUNCTIONS =======================

def init_spark(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[2]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,io.delta:delta-core_2.12:2.4.0") \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

def read_bronze(spark: SparkSession, path: str):
    return spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .load(path)

def build_schema(fields: list) -> ArrayType:
    return ArrayType(StructType([StructField(name, dtype()) for name, dtype in fields]))

def parse_json(df, cols: dict):
    for name, schema in cols.items():
        df = df.withColumn(name, from_json(col(name), schema))
    return df

def explode_data(df, array_col: str, base_cols: list, fill_col: str = "credit_id"):
    df = df.select(*base_cols, explode_outer(array_col).alias(array_col))
    nested_cols = [f"{array_col}.{f.name}" for f in df.select(array_col).schema.fields[0].dataType.fields]
    df = df.select(*base_cols, *nested_cols)
    df = df.withColumn("movie_id", col("movie_id").cast("string"))
    df = df.fillna({fill_col: "0000000000"})
    return df

def create_delta_table(spark, path: str, schema: StructType):
    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)
    return DeltaTable.forPath(spark, path)

def merge_delta(delta_tbl, new_df, keys: list):
    cond = " AND ".join([f"old.{k} = new.{k}" for k in keys])
    delta_tbl.alias("old").merge(
        new_df.alias("new"),
        cond
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# ======================= MAIN =======================

spark = init_spark("credits-silver")

# Load bronze data
df_raw = read_bronze(spark, BRONZE_PATH)

# Define schemas
cast_schema = build_schema([
    ("cast_id", IntegerType),
    ("character", StringType),
    ("credit_id", StringType),
    ("gender", IntegerType),
    ("id", IntegerType),
    ("name", StringType)
])

crew_schema = build_schema([
    ("credit_id", StringType),
    ("department", StringType),
    ("gender", IntegerType),
    ("id", IntegerType),
    ("job", StringType),
    ("name", StringType)
])

# Parse JSON
df_tfm = parse_json(df_raw, {"cast": cast_schema, "crew": crew_schema})

# Explode cast and crew
df_cast = explode_data(df_tfm, "cast", ["movie_id", "title"])
df_crew = explode_data(df_tfm, "crew", ["movie_id", "title"])

# Remove duplicates
df_cast = df_cast.dropDuplicates(["movie_id", "title", "cast_id", "character", "credit_id", "id", "name"])
df_crew = df_crew.dropDuplicates(["movie_id", "id", "credit_id"])

# Create and merge cast delta table
cast_schema_final = df_cast.schema
cast_delta = create_delta_table(spark, SILVER_CAST_PATH, cast_schema_final)
merge_delta(cast_delta, df_cast, ["movie_id", "credit_id"])

# Create and merge crew delta table
crew_schema_final = df_crew.schema
crew_delta = create_delta_table(spark, SILVER_CREW_PATH, crew_schema_final)
merge_delta(crew_delta, df_crew, ["movie_id", "credit_id"])

print("\n✅ Successfully migrated Cast and Crew data to Silver Layer.")
