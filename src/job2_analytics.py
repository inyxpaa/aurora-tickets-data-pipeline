from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum, when, lit, count

spark = SparkSession.builder \
    .appName("Job2_Analytics") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

ruta_curated = "s3a://aurora-tickets-inigo/aurora/Inigo/curated"
ruta_analytics = "s3a://aurora-tickets-inigo/aurora/Inigo/analytics"

jdbc_url = "jdbc:mysql://auroradb.cfi2yqyse3sp.us-east-1.rds.amazonaws.com:3306/aurora"
propiedades_jdbc = {
    "user": "admin",
    "password": "adminpassword",
    "driver": "com.mysql.cj.jdbc.Driver"
}

df_clickstream = spark.read.parquet(f"{ruta_curated}/clickstream")
df_transactions = spark.read.parquet(f"{ruta_curated}/transactions")
df_events = spark.read.parquet(f"{ruta_curated}/events")

df_funnel = df_clickstream.groupBy("dt").agg(
    countDistinct("session_id").alias("sessions_total"),
    countDistinct(when(col("event_type") == "list", col("session_id"))).alias("sessions_event_list"),
    countDistinct(when(col("event_type") == "detail", col("session_id"))).alias("sessions_event_detail"),
    countDistinct(when(col("event_type") == "checkout", col("session_id"))).alias("sessions_begin_checkout"),
    countDistinct(when(col("event_type") == "purchase", col("session_id"))).alias("sessions_purchase")
).withColumn("conversion_rate", col("sessions_purchase") / col("sessions_total"))

df_funnel.write.mode("overwrite").parquet(f"{ruta_analytics}/funnel_daily")
df_funnel.write.jdbc(url=jdbc_url, table="metrics_funnel_daily", mode="overwrite", properties=propiedades_jdbc)

df_rank = df_clickstream.filter(col("event_type") == "detail").groupBy("dt", "event_id").agg(
    count("session_id").alias("detail_views")
).join(
    df_transactions.groupBy("dt", "event_id").agg(
        count("transaction_id").alias("purchases"),
        sum("amount").alias("revenue_total")
    ),
    ["dt", "event_id"],
    "left"
).fillna(0).withColumn("interest_to_purchase_ratio", col("detail_views") / (col("purchases") + 1))

df_rank.write.mode("overwrite").parquet(f"{ruta_analytics}/event_rank")
df_rank.write.jdbc(url=jdbc_url, table="metrics_event_rank", mode="overwrite", properties=propiedades_jdbc)

df_anomalies = df_clickstream.groupBy("dt", "ip").agg(
    count("event_id").alias("requests")
).withColumn(
    "is_anomaly", when(col("requests") > 1000, True).otherwise(False)
).withColumn(
    "reason", when(col("is_anomaly"), lit("Alta tasa de peticiones")).otherwise(lit(""))
).filter(col("is_anomaly") == True)

df_anomalies.write.mode("overwrite").parquet(f"{ruta_analytics}/anomalies")
df_anomalies.write.jdbc(url=jdbc_url, table="metrics_anomalies", mode="overwrite", properties=propiedades_jdbc)

spark.stop()
