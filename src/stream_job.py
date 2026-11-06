"""Structured Streaming: fonte (Kafka | arquivos) -> Bronze -> Silver -> Gold (janelas).

- Bronze: payload bruto + metadados, append-only.
- Silver: parse com schema, validação, deduplicação por event_id com watermark.
- Gold: receita por categoria em janelas tumbling de 5 min (event time).
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.events import ORDER_SCHEMA

WATERMARK = "30 minutes"
WINDOW = "5 minutes"


def get_spark(app: str = "streaming-orders") -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName(app)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def read_source(spark: SparkSession, source: str, path: str) -> DataFrame:
    """Kafka em produção/docker; arquivos JSON como fonte local reprodutível."""
    if source == "kafka":
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
            .option("subscribe", os.getenv("KAFKA_TOPIC", "orders"))
            .option("startingOffsets", "earliest")
            .load()
            .select(F.col("value").cast("string").alias("raw"), F.col("timestamp").alias("ingest_time"))
        )
    return (
        spark.readStream.format("text")
        .load(path)
        .select(F.col("value").alias("raw"), F.current_timestamp().alias("ingest_time"))
    )


def to_silver(raw: DataFrame) -> DataFrame:
    """Parse + validação + deduplicação. Função pura -> testável com DataFrame estático."""
    parsed = raw.select(F.from_json("raw", ORDER_SCHEMA).alias("e"), "ingest_time").select("e.*", "ingest_time")
    valid = parsed.filter(
        F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & (F.col("amount") > 0)
        & (F.col("quantity") > 0)
    )
    return valid.withWatermark("event_time", WATERMARK).dropDuplicates(["event_id"])


def to_gold(silver: DataFrame) -> DataFrame:
    return (
        silver.withWatermark("event_time", WATERMARK)
        .groupBy(F.window("event_time", WINDOW).alias("w"), "category")
        .agg(F.sum("amount").alias("revenue"), F.count("*").alias("orders"))
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "category",
            F.round("revenue", 2).alias("revenue"),
            "orders",
        )
    )


def run(source: str, input_path: str, base: str, once: bool = True) -> dict:
    spark = get_spark()
    raw = read_source(spark, source, input_path)
    trigger = {"availableNow": True} if once else {"processingTime": "10 seconds"}

    q_bronze = (
        raw.writeStream.format("parquet")
        .option("path", f"{base}/bronze")
        .option("checkpointLocation", f"{base}/_chk/bronze")
        .outputMode("append")
        .trigger(**trigger)
        .start()
    )
    q_bronze.awaitTermination()

    bronze = spark.readStream.format("parquet").schema(raw.schema).load(f"{base}/bronze")
    silver = to_silver(bronze)
    q_silver = (
        silver.writeStream.format("parquet")
        .option("path", f"{base}/silver")
        .option("checkpointLocation", f"{base}/_chk/silver")
        .outputMode("append")
        .trigger(**trigger)
        .start()
    )
    q_silver.awaitTermination()

    silver_df = spark.read.parquet(f"{base}/silver")
    gold = to_gold(silver_df.drop("ingest_time"))  # batch sobre Silver já deduplicada
    gold.write.mode("overwrite").parquet(f"{base}/gold")
    return {
        "bronze": spark.read.parquet(f"{base}/bronze").count(),
        "silver": silver_df.count(),
        "gold": spark.read.parquet(f"{base}/gold").count(),
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["files", "kafka"], default="files")
    ap.add_argument("--input", default="data/landing")
    ap.add_argument("--base", default="data/lake")
    a = ap.parse_args()
    print(run(a.source, a.input, a.base))
