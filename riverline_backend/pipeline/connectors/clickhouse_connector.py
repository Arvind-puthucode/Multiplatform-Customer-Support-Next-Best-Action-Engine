from typing import List, Dict, Optional
import clickhouse_connect
import json
from datetime import datetime
import uuid
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
                created_at DateTime64(3) DEFAULT now()
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(interaction_timestamp)
            ORDER BY (platform_type, interaction_timestamp, external_id)
            ''')
            self.client.command('''
            CREATE TABLE IF NOT EXISTS ingestion_watermarks (
                id String,
                pipeline_name String,
                platform_type String,
                last_processed_timestamp DateTime64(3),
                last_processed_external_id String,
                records_processed UInt64,
                last_run_status String,
                updated_at DateTime64(3) DEFAULT now()
            )
            ENGINE = MergeTree()
            ORDER BY (pipeline_name, platform_type, updated_at)
            ''')
            self.client.command('''
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id String,
                pipeline_name String,
                run_type LowCardinality(String),
                database_target LowCardinality(String),
                start_timestamp DateTime64(3),
                end_timestamp DateTime64(3),
                status LowCardinality(String),
                records_processed UInt64,
                error_message String,
                config_snapshot String,
                created_at DateTime64(3) DEFAULT now()
            )
            ENGINE = MergeTree()
            ORDER BY (start_timestamp, run_id)
            ''')
            self.client.command('''
            CREATE TABLE IF NOT EXISTS conversations (
                conversation_id String,
                customer_id String,
                brand_account_id String,
                conversation_start_timestamp DateTime64(3),
                conversation_end_timestamp DateTime64(3),
                total_interactions UInt32,
                resolution_status LowCardinality(String),
                customer_sentiment_score Decimal32(4),
                conversation_topic String,
                channel_mix String,
                created_at DateTime64(3) DEFAULT now()
            )
            ENGINE = MergeTree()
            ORDER BY (conversation_start_timestamp, customer_id)
            ''')
            self.client.command('''
            CREATE TABLE IF NOT EXISTS customer_profiles (
                customer_id String,
                platform_accounts String,
                interaction_history_summary String,
                behavioral_tags String,
                preferred_channels String,
                avg_response_time Decimal32(2),
                total_conversations UInt32,
                resolution_rate Decimal32(2),
                last_interaction_timestamp DateTime64(3),
                created_at DateTime64(3) DEFAULT now()
            )
            ENGINE = MergeTree()
            ORDER BY (customer_id, created_at)
            ''')
            return True
        except Exception as e:
            print(f"Error creating tables: {e}")
            return False

    def batch_insert(self, records: List[Dict]) -> bool:
        try:
            # Convert datetime objects and dicts to string for ClickHouse
            processed_records = []
            for record in records:
                processed_record = record.copy()
                processed_record['content_metadata'] = json.dumps(processed_record.get('content_metadata', {}))
                processed_record['platform_metadata'] = json.dumps(processed_record.get('platform_metadata', {}))
                processed_record['processing_metadata'] = json.dumps(processed_record.get('processing_metadata', {}))
                processed_record['interaction_id'] = str(processed_record.get('interaction_id', ''))
                processed_record['external_id'] = str(processed_record.get('external_id', ''))
                processed_record['participant_external_id'] = str(processed_record.get('participant_external_id', ''))
                processed_record['parent_interaction_id'] = str(processed_record.get('parent_interaction_id', ''))
                processed_record['created_at'] = processed_record.get('created_at', datetime.now())
                
                for key, value in processed_record.items():
                    if isinstance(value, datetime):
                        processed_record[key] = value.isoformat()
                    elif isinstance(value, dict):
                        processed_record[key] = json.dumps(value)
                processed_records.append(processed_record)
            column_names = [
                'interaction_id', 'external_id', 'platform_type', 'participant_external_id',
                'content_text', 'content_metadata', 'interaction_timestamp', 'parent_interaction_id',
                'platform_metadata', 'processing_metadata', 'created_at'
            ]
            data_to_insert = []
            for record in processed_records:
                row = [
                    record.get('interaction_id'),
                    record.get('external_id'),
                    record.get('platform_type'),
                    record.get('participant_external_id'),
                    record.get('content_text', ''),
                    record.get('content_metadata'),
                    record.get('interaction_timestamp'),
                    record.get('parent_interaction_id'),
                    record.get('platform_metadata'),
                    record.get('processing_metadata'),
                    record.get('created_at')
                ]
                data_to_insert.append(row)
            self.client.insert('interactions', data_to_insert, column_names=column_names)
            return True
        except Exception as e:
            print(f"Error batch inserting records: {e}")
            import traceback
            traceback.print_exc()
            return False

    def get_last_watermark(self, pipeline_name: str, platform_type: str) -> Optional[str]:
        try:
            result = self.client.query(
                f"SELECT last_processed_timestamp FROM ingestion_watermarks WHERE pipeline_name = '{pipeline_name}' AND platform_type = '{platform_type}' ORDER BY updated_at DESC LIMIT 1"
            )
            if result.row_count > 0:
                return result.first_row[0].isoformat()
            return None
        except Exception as e:
            print(f"Error getting last watermark from ClickHouse: {e}")
            return None

    def update_watermark(self, pipeline_name: str, platform_type: str, timestamp: str, records_processed: int, status: str) -> bool:
        try:
            # Always insert a new watermark record
            column_names = [
                'id', 'pipeline_name', 'platform_type', 'last_processed_timestamp',
                'last_processed_external_id', 'records_processed', 'last_run_status', 'updated_at'
            ]
            data_to_insert = [
                str(uuid.uuid4()),
                pipeline_name,
                platform_type,
                timestamp,
                '',
                records_processed,
                status,
                datetime.now().isoformat()
            ]
            self.client.insert('ingestion_watermarks', [data_to_insert], column_names=column_names)
            return True
        except Exception as e:
            print(f"Error updating watermark in ClickHouse: {e}")
            import traceback
            traceback.print_exc()
            return False

    def health_check(self) -> bool:
        return self.client.ping()

    def log_pipeline_run_start(self, run_data: Dict) -> str:
        try:
            processed_run_data = run_data.copy()
            processed_run_data['config_snapshot'] = json.dumps(processed_run_data.get('config_snapshot', {}))
            processed_run_data['end_timestamp'] = processed_run_data.get('end_timestamp', datetime(1970, 1, 1)).isoformat() # Default for nullable
            processed_run_data['records_processed'] = processed_run_data.get('records_processed', 0)
            processed_run_data['error_message'] = processed_run_data.get('error_message', '')
            processed_run_data['created_at'] = processed_run_data.get('created_at', datetime.now()).isoformat()

            column_names = [
                'run_id', 'pipeline_name', 'run_type', 'database_target', 'start_timestamp',
                'end_timestamp', 'status', 'records_processed', 'error_message', 'config_snapshot',
                'created_at'
            ]
            data_to_insert = [
                processed_run_data.get('run_id'),
                processed_run_data.get('pipeline_name'),
                processed_run_data.get('run_type'),
                processed_run_data.get('database_target'),
                processed_run_data.get('start_timestamp'),
                processed_run_data.get('end_timestamp'),
                processed_run_data.get('status'),
                processed_run_data.get('records_processed'),
                processed_run_data.get('error_message'),
                processed_run_data.get('config_snapshot'),
                processed_run_data.get('created_at')
            ]
            print(f"Attempting to insert into pipeline_runs: data={data_to_insert}, columns={column_names}")
            insert_result = self.client.insert('pipeline_runs', [data_to_insert], column_names=column_names)
            print(f"Insert result for pipeline_runs: {insert_result}")
            return run_data['run_id']
        except Exception as e:
            print(f"Error logging pipeline run start to ClickHouse: {e}")
            import traceback
            traceback.print_exc()
            return None

    def log_pipeline_run_end(self, run_id: str, end_data: Dict) -> bool:
        try:
            processed_end_data = end_data.copy()
            processed_end_data['performance_metrics'] = json.dumps(processed_end_data.get('performance_metrics', {}))
            processed_end_data['error_message'] = processed_end_data.get('error_message', '')
            processed_end_data['records_processed'] = processed_end_data.get('records_processed', 0)

            # Fetch the original run data to merge with end_data
            original_run_data_result = self.client.query(f"SELECT * FROM pipeline_runs WHERE run_id = '{run_id}' ORDER BY created_at DESC LIMIT 1")

            if original_run_data_result.result_rows:
                columns = original_run_data_result.column_names
                original_run_dict = dict(zip(columns, original_run_data_result.result_rows[0]))
                
                merged_data = {**original_run_dict, **processed_end_data}
                
                # Ensure timestamps are in correct format for ClickHouse
                for ts_key in ['start_timestamp', 'end_timestamp', 'created_at']:
                    if ts_key in merged_data:
                        if isinstance(merged_data[ts_key], datetime):
                            merged_data[ts_key] = merged_data[ts_key].isoformat()
                        elif isinstance(merged_data[ts_key], str):
                            pass
                        else:
                            merged_data[ts_key] = datetime(1970, 1, 1).isoformat() # Default for other types

                # Ensure all other fields are present and correctly formatted
                merged_data['error_message'] = str(merged_data.get('error_message', ''))
                merged_data['config_snapshot'] = json.dumps(merged_data.get('config_snapshot', {}))
                merged_data['performance_metrics'] = json.dumps(merged_data.get('performance_metrics', {})) # This field is not in schema, but was in original code

                # Convert merged_data to list of lists for insertion
                column_names = [
                    'run_id', 'pipeline_name', 'run_type', 'database_target', 'start_timestamp',
                    'end_timestamp', 'status', 'records_processed', 'error_message', 'config_snapshot',
                    'created_at'
                ]
                data_to_insert = [
                    str(merged_data.get('run_id', '')),
                    str(merged_data.get('pipeline_name', '')),
                    str(merged_data.get('run_type', '')),
                    str(merged_data.get('database_target', '')),
                    merged_data.get('start_timestamp'),
                    merged_data.get('end_timestamp'),
                    str(merged_data.get('status', '')),
                    merged_data.get('records_processed'),
                    str(merged_data.get('error_message', '')),
                    str(merged_data.get('config_snapshot', '')),
                    merged_data.get('created_at')
                ]
                self.client.insert('pipeline_runs', [data_to_insert], column_names=column_names)
            else:
                print(f"Warning: Original run data not found for run_id {run_id}. Inserting end data as new record.")
                # If original not found, create a new record with available end_data and run_id
                new_record = {
                    'run_id': run_id,
                    'pipeline_name': "unknown", # Default if original not found
                    'run_type': "unknown",
                    'database_target': "unknown",
                    'start_timestamp': datetime(1970, 1, 1).isoformat(),
                    'end_timestamp': processed_end_data.get('end_timestamp', datetime.now()).isoformat(),
                    'status': processed_end_data.get('status', 'failed'),
                    'records_processed': processed_end_data.get('records_processed', 0),
                    'error_message': processed_end_data.get('error_message', ''),
                    'config_snapshot': json.dumps({}),
                    'created_at': datetime.now().isoformat()
                }
                # Convert new_record to list of lists for insertion
                column_names = [
                    'run_id', 'pipeline_name', 'run_type', 'database_target', 'start_timestamp',
                    'end_timestamp', 'status', 'records_processed', 'error_message', 'config_snapshot',
                    'created_at'
                ]
                data_to_insert = [
                    str(new_record.get('run_id', '')),
                    str(new_record.get('pipeline_name', '')),
                    str(new_record.get('run_type', '')),
                    str(new_record.get('database_target', '')),
                    new_record.get('start_timestamp'),
                    new_record.get('end_timestamp'),
                    str(new_record.get('status', '')),
                    new_record.get('records_processed'),
                    str(new_record.get('error_message', '')),
                    str(new_record.get('config_snapshot', '')),
                    new_record.get('created_at')
                ]
                self.client.insert('pipeline_runs', [data_to_insert], column_names=column_names)

            return True
        except Exception as e:
            print(f"Error logging pipeline run end to ClickHouse: {e}")
            return False

    def fetch_interactions(self) -> List[Dict]:
        try:
            result = self.client.query("SELECT * FROM interactions")
            if not result.result_rows:
                return []
            columns = result.column_names
            return [dict(zip(columns, row)) for row in result.result_rows]
        except Exception as e:
            print(f"Error fetching interactions from ClickHouse: {e}")
            return []

    def fetch_customer_profiles(self) -> List[Dict]:
        try:
            result = self.client.query("SELECT * FROM customer_profiles") # Assuming customer_profiles table exists in ClickHouse
            if not result.result_rows:
                return []
            columns = result.column_names
            return [dict(zip(columns, row)) for row in result.result_rows]
        except Exception as e:
            print(f"Error fetching customer profiles from ClickHouse: {e}")
            return []

    def batch_insert_conversations(self, records: List[Dict]) -> bool:
        try:
            processed_records = []
            for record in records:
                processed_record = record.copy()
                # Convert datetime objects and dicts to string for ClickHouse
                for key, value in processed_record.items():
                    if isinstance(value, datetime):
                        processed_record[key] = value.isoformat()
                    elif isinstance(value, dict):
                        processed_record[key] = json.dumps(value)
                processed_records.append(processed_record)
            
            column_names = [
                'conversation_id', 'customer_id', 'brand_account_id', 'conversation_start_timestamp',
                'conversation_end_timestamp', 'total_interactions', 'resolution_status',
                'customer_sentiment_score', 'conversation_topic', 'channel_mix', 'created_at'
            ]
            data_to_insert = []
            for record in processed_records:
                row = [
                    record.get('conversation_id'),
                    record.get('customer_id'),
                    record.get('brand_account_id'),
                    record.get('conversation_start_timestamp'),
                    record.get('conversation_end_timestamp'),
                    record.get('total_interactions'),
                    record.get('resolution_status'),
                    record.get('customer_sentiment_score'),
                    record.get('conversation_topic'),
                    record.get('channel_mix'),
                    record.get('created_at')
                ]
                data_to_insert.append(row)
            self.client.insert('conversations', data_to_insert, column_names=column_names)
            return True
        except Exception as e:
            print(f"Error batch inserting conversations to ClickHouse: {e}")
            import traceback
            traceback.print_exc()
            return False

    def batch_insert_customer_profiles(self, records: List[Dict]) -> bool:
        try:
            processed_records = []
            for record in records:
                processed_record = record.copy()
                # Convert datetime objects and dicts to string for ClickHouse
                for key, value in processed_record.items():
                    if isinstance(value, datetime):
                        processed_record[key] = value.isoformat()
                    elif isinstance(value, dict):
                        processed_record[key] = json.dumps(value)
                processed_records.append(processed_record)
            
            column_names = [
                'customer_id', 'platform_accounts', 'interaction_history_summary',
                'behavioral_tags', 'preferred_channels', 'avg_response_time',
                'total_conversations', 'resolution_rate', 'last_interaction_timestamp',
                'created_at'
            ]
            data_to_insert = []
            for record in processed_records:
                row = [
                    record.get('customer_id'),
                    record.get('platform_accounts'),
                    record.get('interaction_history_summary'),
                    record.get('behavioral_tags'),
                    record.get('preferred_channels'),
                    record.get('avg_response_time'),
                    record.get('total_conversations'),
                    record.get('resolution_rate'),
                    record.get('last_interaction_timestamp'),
                    record.get('created_at')
                ]
                data_to_insert.append(row)
            self.client.insert('customer_profiles', data_to_insert, column_names=column_names)
            return True
        except Exception as e:
            print(f"Error batch inserting customer profiles to ClickHouse: {e}")
            import traceback
            traceback.print_exc()
            return False
