
from typing import List, Dict, Optional
import clickhouse_connect
from .db_connector import DatabaseConnector

class ClickHouseConnector(DatabaseConnector):
    def __init__(self, host: str, port: int, user: str, password: str):
        self.client = clickhouse_connect.get_client(host=host, port=port, user=user, password=password)

    def connect(self) -> bool:
        return self.client.ping()

    def create_tables(self) -> bool:
        try:
            self.client.command('''
            CREATE TABLE IF NOT EXISTS interactions (
                interaction_id String,
                external_id String,
                platform_type LowCardinality(String),
                participant_external_id String,
                content_text String,
                content_metadata String, -- JSON as String
                interaction_timestamp DateTime64(3),
                parent_interaction_id String,
                platform_metadata String, -- JSON as String
                processing_metadata String, -- JSON as String
                created_at DateTime64(3)
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(interaction_timestamp)
            ORDER BY (platform_type, interaction_timestamp, external_id)
            ''')
            return True
        except Exception as e:
            print(f"Error creating tables: {e}")
            return False

    def batch_insert(self, records: List[Dict]) -> bool:
        try:
            self.client.insert('interactions', records)
            return True
        except Exception as e:
            print(f"Error batch inserting records: {e}")
            return False

    def get_last_watermark(self) -> Optional[str]:
        # Implementation for getting the last watermark from ClickHouse
        pass

    def update_watermark(self, value: str) -> bool:
        # Implementation for updating the watermark in ClickHouse
        pass

    def health_check(self) -> bool:
        return self.client.ping()
