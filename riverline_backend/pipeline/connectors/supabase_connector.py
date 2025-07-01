
from typing import List, Dict, Optional
from supabase import create_client, Client
from postgrest import APIResponse
from .db_connector import DatabaseConnector
import json
from datetime import datetime
import uuid

class SupabaseConnector(DatabaseConnector):
    def __init__(self, url: str, key: str):
        self.client: Client = create_client(url, key)

    def connect(self) -> bool:
        # Supabase client doesn't have a connect method, connection is on-demand
        return True

    def create_tables(self) -> bool:
        try:
            # Using the Supabase client to execute raw SQL is not directly supported.
            # It's recommended to use the Supabase dashboard or a migration tool.
            # For the purpose of this hackathon, we'll assume tables are created.
            return True
        except Exception as e:
            print(f"Error creating tables: {e}")
            return False

    def batch_insert(self, records: List[Dict]) -> bool:
        try:
            processed_records = []
            for record in records:
                processed_record = record.copy()
                # Ensure all datetime objects are ISO formatted strings
                for key, value in processed_record.items():
                    if isinstance(value, datetime):
                        processed_record[key] = value.isoformat()
                    elif isinstance(value, dict):
                        processed_record[key] = json.dumps(value)
                
                # Filter out any columns not in the target schema
                # This is a safeguard against schema mismatches
                valid_columns = [
                    'interaction_id', 'external_id', 'platform_type', 'participant_external_id',
                    'content_text', 'content_metadata', 'interaction_timestamp', 'parent_interaction_id',
                    'platform_metadata', 'processing_metadata', 'created_at'
                ]
                filtered_record = {k: v for k, v in processed_record.items() if k in valid_columns}
                processed_records.append(filtered_record)
            response: APIResponse = self.client.table('interactions').insert(processed_records).execute()
            return response.data is not None
        except Exception as e:
            print(f"Error batch inserting records: {e}")
            return False

    def batch_insert_conversations(self, records: List[Dict]) -> bool:
        try:
            response: APIResponse = self.client.table('conversations').insert(records).execute()
            return response.data is not None
        except Exception as e:
            print(f"Error batch inserting conversations: {e}")
            return False

    def batch_insert_customer_profiles(self, records: List[Dict]) -> bool:
        try:
            response: APIResponse = self.client.table('customer_profiles').insert(records).execute()
            return response.data is not None
        except Exception as e:
            print(f"Error batch inserting customer profiles: {e}")
            return False

    def get_last_watermark(self, pipeline_name: str, platform_type: str) -> Optional[str]:
        try:
            response = self.client.table('ingestion_watermarks')\
                .select('last_processed_timestamp')\
                .eq('pipeline_name', pipeline_name)\
                .eq('platform_type', platform_type)\
                .order('updated_at', desc=True)\
                .limit(1).execute()
            
            if response.data:
                return response.data[0]['last_processed_timestamp']
            return None
        except Exception as e:
            print(f"Error getting last watermark: {e}")
            return None

    def update_watermark(self, pipeline_name: str, platform_type: str, timestamp: str, records_processed: int, status: str) -> bool:
        try:
            # Check if a watermark already exists for this pipeline and platform
            existing_watermark = self.client.table('ingestion_watermarks')\
                .select('id')\
                .eq('pipeline_name', pipeline_name)\
                .eq('platform_type', platform_type)\
                .execute()

            if existing_watermark.data:
                # Update existing watermark
                response = self.client.table('ingestion_watermarks')\
                    .update({
                        'last_processed_timestamp': timestamp,
                        'records_processed': records_processed,
                        'last_run_status': status,
                        'updated_at': datetime.now().isoformat()
                    })\
                    .eq('id', existing_watermark.data[0]['id'])\
                    .execute()
            else:
                # Insert new watermark
                response = self.client.table('ingestion_watermarks')\
                    .insert({
                        'id': str(uuid.uuid4()),
                        'pipeline_name': pipeline_name,
                        'platform_type': platform_type,
                        'last_processed_timestamp': timestamp,
                        'records_processed': records_processed,
                        'last_run_status': status,
                        'updated_at': datetime.now().isoformat()
                    })\
                    .execute()
            
            return response.data is not None
        except Exception as e:
            print(f"Error updating watermark: {e}")
            return False

    def health_check(self) -> bool:
        # Implementation for health check
        pass

    def fetch_interactions(self) -> List[Dict]:
        try:
            response = self.client.table('interactions').select('*').execute()
            return response.data
        except Exception as e:
            print(f"Error fetching interactions: {e}")
            return []

    def log_pipeline_run_start(self, run_data: Dict) -> str:
        try:
            processed_run_data = run_data.copy()
            for key, value in processed_run_data.items():
                if isinstance(value, datetime):
                    processed_run_data[key] = value.isoformat()
                elif isinstance(value, dict):
                    processed_run_data[key] = json.dumps(value)
            response = self.client.table('pipeline_runs').insert(processed_run_data).execute()
            if response.data:
                return response.data[0]['run_id']
            return None
        except Exception as e:
            print(f"Error logging pipeline run start: {e}")
            return None

    def log_pipeline_run_end(self, run_id: str, end_data: Dict) -> bool:
        try:
            processed_end_data = end_data.copy()
            for key, value in processed_end_data.items():
                if isinstance(value, datetime):
                    processed_end_data[key] = value.isoformat()
                elif isinstance(value, dict):
                    processed_end_data[key] = json.dumps(value)
            response = self.client.table('pipeline_runs').update(processed_end_data).eq('run_id', run_id).execute()
            return response.data is not None
        except Exception as e:
            print(f"Error logging pipeline run end: {e}")
            return False

    def fetch_customer_profiles(self) -> List[Dict]:
        try:
            response = self.client.table('customer_profiles').select('*').execute()
            return response.data
        except Exception as e:
            print(f"Error fetching customer profiles: {e}")
            return []
