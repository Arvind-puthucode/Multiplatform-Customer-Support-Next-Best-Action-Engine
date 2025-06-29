"""
Pipeline configuration management
Simple settings for the ingestion pipeline
"""

import os
from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
from typing import Optional
class PipelineConfig(BaseSettings):
    """Basic pipeline configuration"""

    # Supabase connection
    supabase_url: str = Field(..., description="Supabase URL", env="SUPABASE_URL")
    supabase_key: str = Field(..., description="Supabase Key", env="SUPABASE_KEY")

    # Data source
    csv_path: str = Field("data/customer_support_twitter.csv", description="CSV Path", env="CSV_PATH")

    # Processing settings
    batch_size: int = Field(1000, description="Batch size", env="BATCH_SIZE")
    max_text_length: int = Field(1000, description="Maximum text length", env="MAX_TEXT_LENGTH")
    min_text_length: int = Field(5, description="Minimum text length", env="MIN_TEXT_LENGTH")

    # Logging
    log_level: str = Field("INFO", description="Log level", env="LOG_LEVEL")



    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"