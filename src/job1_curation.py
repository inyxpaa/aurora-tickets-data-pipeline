from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("Job1_Curation") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

ruta_base = "s3a://aurora-tickets-inigo/aurora/Inigo"

df_events = spark.read.option("header", "true").csv(f"{ruta_base}/raw/events.csv")
df_campaigns = spark.read.option("header", "true").csv(f"{ruta_base}/raw/campaigns.csv")
df_transactions = spark.read.option("header", "true").csv(f"{ruta_base}/raw/transactions.csv")
df_clickstream = spark.read.json(f"{ruta_base}/raw/aurora_clickstream.jsonl")

df_events_clean = df_events.dropna(subset=["event_id"])

df_campaigns_clean = df_campaigns.dropna(subset=["utm_campaign"])

df_transactions_clean = df_transactions.dropna(subset=["transaction_id", "session_id", "event_id"]) \
    .filter(col("amount") > 0) \
    .join(df_events_clean, "event_id", "left_semi")

df_clickstream_clean = df_clickstream.dropna(subset=["timestamp", "session_id"]) \
    .withColumn("dt", to_date(col("timestamp"))) \
    .filter(col("dt").isNotNull())

df_events_clean.write.mode("overwrite").parquet(f"{ruta_base}/curated/events")

df_campaigns_clean.write.mode("overwrite").parquet(f"{ruta_base}/curated/campaigns")

df_transactions_clean.withColumn("dt", to_date(col("timestamp"))) \
    .write.mode("overwrite").partitionBy("dt").parquet(f"{ruta_base}/curated/transactions")

df_clickstream_clean.write.mode("overwrite").partitionBy("dt", "event_type").parquet(f"{ruta_base}/curated/clickstream")

spark.stop()
