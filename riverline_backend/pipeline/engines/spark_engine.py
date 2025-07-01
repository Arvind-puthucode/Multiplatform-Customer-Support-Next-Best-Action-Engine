from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from pyspark.sql import SparkSession, DataFrame
import uuid
from pyspark.sql.functions import col, lit, current_timestamp, from_unixtime, to_timestamp, udf, regexp_replace, when
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DecimalType, IntegerType, BooleanType
import json
from datetime import datetime
import re

from .base_engine import BaseDataEngine

def generate_uuid():
    return str(uuid.uuid4())

class SparkDataEngine(BaseDataEngine):
    def __init__(self, app_name: str = "RiverlineSparkPipeline", master: str = "local[*]", config: Optional[Dict] = None):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.driver.memory", config.get("spark_driver_memory", "4g")) \
            .config("spark.executor.memory", config.get("spark_executor_memory", "4g")) \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN") # Reduce verbosity
        self.uuid_udf = udf(generate_uuid, StringType())

    def read_data(self, file_path: str, last_processed_timestamp: Optional[str] = None) -> DataFrame:
        schema = StructType([
            StructField("tweet_id", StringType(), True),
            StructField("author_id", StringType(), True),
            StructField("inbound", StringType(), True),
            StructField("created_at", StringType(), True), # Read as string, convert later
            StructField("text", StringType(), True),
            StructField("response_tweet_id", StringType(), True),
            StructField("in_response_to_tweet_id", StringType(), True)
        ])

        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .csv(file_path)
        
        # Convert created_at to TimestampType and rename to interaction_timestamp
        df = df.withColumn("interaction_timestamp", 
                           to_timestamp(col("created_at"), "EEE MMM dd HH:mm:ss Z yyyy"))
        
        # Drop the original created_at column
        df = df.drop("created_at")

        if last_processed_timestamp:
            last_ts = datetime.fromisoformat(last_processed_timestamp)
            df = df.filter(col("interaction_timestamp") > lit(last_ts))
        
        return df

    def normalize_data(self, df: DataFrame) -> DataFrame:
        # Apply universal schema mapping and basic cleaning
        df = df.withColumnRenamed("tweet_id", "external_id")                .withColumnRenamed("author_id", "participant_external_id")                .withColumnRenamed("text", "content_text")                .withColumn("interaction_id", self.uuid_udf())                .withColumn("platform_type", lit("twitter"))                .withColumn("interaction_type", lit("tweet"))                .withColumn("content_metadata", lit(json.dumps({})))                .withColumn("platform_metadata", lit(json.dumps({})))                .withColumn("processing_metadata", lit(json.dumps({})))                .withColumn("parent_interaction_id", col("in_response_to_tweet_id"))                .withColumn("created_at", current_timestamp())        # Select and reorder columns to match UniversalInteraction schema        df = df.select(            "interaction_id",            "external_id",            "platform_type",            "participant_external_id",            "content_text",            "content_metadata",            "interaction_timestamp",            "parent_interaction_id",            "interaction_type",            "platform_metadata",            "processing_metadata",            "created_at"        )
        return df

    def quality_check(self, df: DataFrame) -> DataFrame:
        # Example quality checks:
        # 1. Required fields present
        df = df.withColumn("is_valid", 
                           (col("external_id").isNotNull()) & 
                           (col("platform_type").isNotNull()) &
                           (col("interaction_timestamp").isNotNull()) &
                           (col("content_text").isNotNull()))
        
        # 2. Text length within bounds (5-10000 chars)
        df = df.withColumn("is_valid", 
                           col("is_valid") & 
                           (col("content_text").cast(StringType()).rlike(".{5,10000}"))) # Check length after casting to string

        # 3. Valid timestamp formats (already handled in read_data by to_timestamp)
        
        # 4. External ID uniqueness within batch (can be done later if needed, or rely on DB unique constraint)
        
        # Filter out invalid records for now, or mark them for quarantine
        # For this implementation, we'll just add a quality_score column
        df = df.withColumn("quality_score", when(col("is_valid"), lit(1.0)).otherwise(lit(0.0)))
        
        return df

    def get_records(self, df: DataFrame) -> List[Dict]:
        # Convert Spark DataFrame to a list of dictionaries
        # This will trigger computation and can be memory intensive for very large DFs
        return [row.asDict(recursive=True) for row in df.collect()]

    def stop(self):
        self.spark.stop()
