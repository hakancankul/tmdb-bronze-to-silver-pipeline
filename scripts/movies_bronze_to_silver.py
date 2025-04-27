import findspark
findspark.init("/opt/spark")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import explode_outer, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, FloatType, DateType
from delta.tables import *
import os

# ======================= CONFIG =======================
ACCESS_KEY = os.getenv("ACCESS_KEY", "dataops")
SECRET_KEY = os.getenv("SECRET_KEY", "Ankara06")
ENDPOINT = os.getenv("ENDPOINT", "http://minio:9000")

BRONZE_MOVIES_PATH = "s3a://tmdb-bronze/movies/"
SILVER_PATHS = {
    "movies": "s3a://tmdb-silver/movies",
    "genres": "s3a://tmdb-silver/genres",
    "keywords": "s3a://tmdb-silver/keywords",
    "prod_companies": "s3a://tmdb-silver/production_companies",
    "prod_countries": "s3a://tmdb-silver/production_countries",
    "spoken_langs": "s3a://tmdb-silver/spoken_languages"
}

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

def read_data(spark: SparkSession, path: str):
    return spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .load(path)

def parse_json_fields(df, fields: dict):
    for col_name, schema in fields.items():
        df = df.withColumn(col_name, from_json(col(col_name), schema))
    return df

def explode_nested_field(df, col_name: str, base_cols: list, fill_defaults: dict):
    df = df.select(*base_cols, explode_outer(col_name).alias(col_name))
    nested_cols = [f"{col_name}.{f.name}" for f in df.select(col_name).schema.fields[0].dataType.fields]
    df = df.select(*base_cols, *nested_cols)
    for field, default in fill_defaults.items():
        df = df.fillna({field: default})
    return df.withColumn("movie_id", col("movie_id").cast("string"))

def create_and_merge_table(spark, path: str, schema: StructType, df_new, join_keys: list):
    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)
    delta_tbl = DeltaTable.forPath(spark, path)
    cond = " AND ".join([f"old.{k} = new.{k}" for k in join_keys])
    delta_tbl.alias("old").merge(
        df_new.alias("new"),
        cond
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# ======================= MAIN =======================

spark = init_spark("movies-silver")

# Load and preprocess
df_movies = read_data(spark, BRONZE_MOVIES_PATH).withColumnRenamed("id", "movie_id")

# Define JSON schemas
genres_schema = ArrayType(StructType([StructField("id", IntegerType()), StructField("name", StringType())]))
keywords_schema = ArrayType(StructType([StructField("id", IntegerType()), StructField("name", StringType())]))
pcompanies_schema = ArrayType(StructType([StructField("id", IntegerType()), StructField("name", StringType())]))
pcountries_schema = ArrayType(StructType([StructField("iso_3166_1", StringType()), StructField("name", StringType())]))
slangs_schema = ArrayType(StructType([StructField("iso_639_1", StringType()), StructField("name", StringType())]))

# Parse JSON fields
df_movies = parse_json_fields(df_movies, {
    "genres": genres_schema,
    "keywords": keywords_schema,
    "production_companies": pcompanies_schema,
    "production_countries": pcountries_schema,
    "spoken_languages": slangs_schema
})

# Prepare main movie table
base_cols = [
    "movie_id", "title", "budget", "homepage", "original_language", "original_title",
    "overview", "popularity", "release_date", "revenue", "runtime", "status",
    "tagline", "vote_average", "vote_count"
]

schema_movies = StructType([
    StructField("movie_id", StringType()), StructField("title", StringType()), StructField("budget", DoubleType()),
    StructField("homepage", StringType()), StructField("original_language", StringType()), StructField("original_title", StringType()),
    StructField("overview", StringType()), StructField("popularity", FloatType()), StructField("release_date", DateType()),
    StructField("revenue", DoubleType()), StructField("runtime", IntegerType()), StructField("status", StringType()),
    StructField("tagline", StringType()), StructField("vote_average", FloatType()), StructField("vote_count", IntegerType())
])

# Type casting
for field in schema_movies.fields:
    df_movies = df_movies.withColumn(field.name, col(field.name).cast(field.dataType))

df_movies_final = df_movies.select(base_cols).dropDuplicates(["movie_id"])

# Explode nested fields
df_genres = explode_nested_field(df_movies, "genres", ["movie_id"], {"id": -9999}).dropDuplicates(["movie_id", "id", "name"])
df_keywords = explode_nested_field(df_movies, "keywords", ["movie_id"], {"id": -9999}).dropDuplicates(["movie_id", "id", "name"])
df_prod_companies = explode_nested_field(df_movies, "production_companies", ["movie_id"], {"id": -9999}).dropDuplicates(["movie_id", "id", "name"])
df_prod_countries = explode_nested_field(df_movies, "production_countries", ["movie_id"], {"iso_3166_1": "XX"}).dropDuplicates(["movie_id", "iso_3166_1"])
df_spoken_langs = explode_nested_field(df_movies, "spoken_languages", ["movie_id"], {"iso_639_1": "XX"}).dropDuplicates(["movie_id", "iso_639_1"])

# Merge all tables
create_and_merge_table(spark, SILVER_PATHS["movies"], schema_movies, df_movies_final, ["movie_id"])

create_and_merge_table(spark, SILVER_PATHS["genres"],
    StructType([StructField("movie_id", StringType()), StructField("id", IntegerType()), StructField("name", StringType())]),
    df_genres, ["movie_id", "id"])

create_and_merge_table(spark, SILVER_PATHS["keywords"],
    StructType([StructField("movie_id", StringType()), StructField("id", IntegerType()), StructField("name", StringType())]),
    df_keywords, ["movie_id", "id"])

create_and_merge_table(spark, SILVER_PATHS["prod_companies"],
    StructType([StructField("movie_id", StringType()), StructField("id", IntegerType()), StructField("name", StringType())]),
    df_prod_companies, ["movie_id", "id"])

create_and_merge_table(spark, SILVER_PATHS["prod_countries"],
    StructType([StructField("movie_id", StringType()), StructField("iso_3166_1", StringType()), StructField("name", StringType())]),
    df_prod_countries, ["movie_id", "iso_3166_1"])

create_and_merge_table(spark, SILVER_PATHS["spoken_langs"],
    StructType([StructField("movie_id", StringType()), StructField("iso_639_1", StringType()), StructField("name", StringType())]),
    df_spoken_langs, ["movie_id", "iso_639_1"])

print("\n✅ Successfully migrated Movies and related tables to Silver Layer.")
