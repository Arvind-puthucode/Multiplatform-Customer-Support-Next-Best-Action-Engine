"""
Configuration management for the ingestion pipeline
"""

import os
from pydantic_settings import BaseSettings
from pydantic import validator, Field
from typing import Optional

class IngestionConfig(BaseSettings):
    """Configuration for the ingestion pipeline"""
    # ClickHouse Database
    clickhouse_host: str = Field("localhost", env="CLICKHOUSE_HOST")
    clickhouse_user: str = Field("default", env="CLICKHOUSE_USER")
    clickhouse_password: str = Field("", env="CLICKHOUSE_PASSWORD")
    clickhouse_database: str = Field("riverline", env="CLICKHOUSE_DATABASE")
    clickhouse_secure: bool = Field(True, env="CLICKHOUSE_SECURE")
    
    # Processing
    batch_size: int = Field(10000, env="BATCH_SIZE")
    max_text_length: int = 1000
    min_text_length: int = 5
    quality_threshold: float = 0.8
    
    # Data Source
    data_source_path: str = Field("data/customer_support_twitter.csv", env="DATA_SOURCE_PATH")
    data_source_type: str = "csv"
    
    # Monitoring
    log_level: str = Field("INFO", env="LOG_LEVEL")
    log_file: str = Field("logs/ingestion_pipeline.log", env="LOG_FILE")

    @validator('clickhouse_host')
    def host_required(cls, v):
        if not v:
            raise ValueError("ClickHouse host is required")
        return v

    @validator('batch_size')
    def batch_size_positive(cls, v):
        if v <= 0:
            raise ValueError("Batch size must be positive")
        return v

    @validator('quality_threshold')
    def quality_threshold_range(cls, v):
        if not (0 <= v <= 1):
            raise ValueError("Quality threshold must be between 0 and 1")
        return v

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"