
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize AWS Glue
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Approved S3 architecture
BUCKET = "wistia-video-analytics-593271843946"

BRONZE_PATH = f"s3://{BUCKET}/bronze/wistia/"
SILVER_PATH = f"s3://{BUCKET}/silver/wistia/media/"

# Read only Bronze media JSON files
bronze_df = (
    spark.read
    .option("multiLine", "true")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "media_*.json")
    .json(BRONZE_PATH)
)

# Extract media records from the records array
media_df = (
    bronze_df
    .select(F.explode("records").alias("record"))
    .select("record.*")
)

# Clean data and safely handle Wistia IDs
clean_df = media_df.select(
    F.col("media_id").cast("string").alias("media_id"),
    F.col("hashed_id").cast("string").alias("hashed_id"),
    F.trim(F.col("title")).alias("title"),
    F.to_timestamp("created_at").alias("created_at"),
    F.to_timestamp("updated_at").alias("updated_at"),
    F.col("duration").cast("double").alias("duration_seconds"),
    F.col("engagement_metrics.load_count")
        .cast("long").alias("load_count"),
    F.col("engagement_metrics.play_count")
        .cast("long").alias("play_count"),
    F.col("engagement_metrics.play_rate")
        .cast("double").alias("play_rate"),
    F.col("engagement_metrics.hours_watched")
        .cast("double").alias("hours_watched"),
    F.col("engagement_metrics.engagement")
        .cast("double").alias("engagement")
)

# Remove invalid records
clean_df = clean_df.filter(
    F.col("hashed_id").isNotNull()
)

# Deduplicate: retain latest record per video
window_spec = (
    Window
    .partitionBy("hashed_id")
    .orderBy(F.col("updated_at").desc_nulls_last())
)

silver_df = (
    clean_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# Materialize and validate before writing
record_count = silver_df.count()

if record_count == 0:
    raise ValueError("No valid Wistia media records found.")

print(f"Valid media records: {record_count}")

# Write Silver Parquet dataset
(
    silver_df
    .write
    .mode("overwrite")
    .parquet(SILVER_PATH)
)

print("Bronze-to-Silver media transformation succeeded.")

# ==========================================================
# FR10: BRONZE VISITORS -> SILVER VISITORS
# ==========================================================

VISITORS_BRONZE_PATH = (
    f"s3://{BUCKET}/bronze/wistia/"
    "year=2026/month=10/day=28/"
    "visitors/run_id=20261028T002220862275Z/"
)

VISITORS_SILVER_PATH = (
    f"s3://{BUCKET}/silver/wistia/visitors/"
)

# Read visitor JSON pages across dated Bronze folders.
# Exclude unrelated media, event, and manifest files.
visitors_raw = (
    spark.read
    .option("multiLine", "true")
    .option("recursiveFileLookup", "true")
    .option(
        "pathGlobFilter",
        "page_*.json"
    )
    .json(VISITORS_BRONZE_PATH)
)

# Extract visitor records from the nested records array.
visitors_flat = (
    visitors_raw
    .filter(F.col("source") == "wistia")
    .select(F.explode("records").alias("visitor"))
    .select("visitor.*")
)

# Select and clean visitor attributes.
visitors_clean = (
    visitors_flat
    .select(
        F.col("visitor_key")
            .cast("string")
            .alias("visitor_id"),

        F.to_timestamp("created_at")
            .alias("first_seen_at"),

        F.to_timestamp("last_active_at")
            .alias("last_seen_at")
    )
    .filter(F.col("visitor_id").isNotNull())
)

# Retain one record per visitor.
visitor_window = (
    Window
    .partitionBy("visitor_id")
    .orderBy(
        F.col("last_seen_at").desc_nulls_last()
    )
)

visitors_silver = (
    visitors_clean
    .withColumn(
        "row_num",
        F.row_number().over(visitor_window)
    )
    .filter(F.col("row_num") == 1)
    .drop("row_num")
    .withColumn(
        "ip_address",
        F.lit(None).cast("string")
    )
    .select(
        "visitor_id",
        "ip_address",
        "first_seen_at",
        "last_seen_at"
    )
)

visitor_count = visitors_silver.count()

print(f"Valid visitor records: {visitor_count}")

if visitor_count == 0:
    raise ValueError("No visitor records found.")

(
    visitors_silver
    .write
    .mode("overwrite")
    .parquet(VISITORS_SILVER_PATH)
)

print("Bronze-to-Silver visitor transformation succeeded.")

# ==========================================================
# FR10: BRONZE MEDIA METRICS -> SILVER ENGAGEMENT
# ==========================================================

ENGAGEMENT_SILVER_PATH = (
    f"s3://{BUCKET}/silver/wistia/engagement/"
)

# Create one media-level engagement summary per video.
# Visitor-level engagement events are not available
# in the media dataset, so visitor_id remains NULL.
engagement_silver = (
    silver_df
    .select(
        F.col("hashed_id").alias("media_hashed_id"),

        F.lit(None).cast("string").alias("visitor_id"),

        F.lit("media_summary").alias("event_type"),

        F.col("updated_at").alias("event_timestamp"),

        F.col("play_count").cast("int").alias("play_count"),

        F.col("play_rate")
            .cast("decimal(18,6)").alias("play_rate"),

        F.col("hours_watched")
            .cast("decimal(18,6)").alias("hours_watched"),

        F.col("engagement")
            .cast("decimal(18,6)").alias("engagement"),

        F.col("load_count").cast("int").alias("load_count"),

        F.current_date().alias("ingestion_date")
    )
    .filter(F.col("media_hashed_id").isNotNull())
)

engagement_count = engagement_silver.count()

print(f"Valid media engagement summaries: {engagement_count}")

if engagement_count == 0:
    raise ValueError("No media engagement summaries found.")

# Write in the exact order of the Redshift COPY columns,
# excluding the identity-generated engagement_id.
(
    engagement_silver
    .write
    .mode("overwrite")
    .parquet(ENGAGEMENT_SILVER_PATH)
)

print("Bronze-to-Silver engagement transformation succeeded.")

job.commit()
