
import os
from datetime import datetime

import fastapi
from dotenv import load_dotenv
from typing import List, Dict

from fastapi import FastAPI

load_dotenv() # Load environment variables from .env file
import uuid
import pandas as pd
from pipeline.data_engine_factory import DataEngineFactory
from pipeline.connectors.supabase_connector import SupabaseConnector
from pipeline.connectors.clickhouse_connector import ClickHouseConnector
from nba.conversation_processor import ConversationProcessor
from nba.nba_engine import NBAEngine

class Pipeline:
    def __init__(self, config):
        self.config = config
        self.data_engine = DataEngineFactory.get_engine(self.config) if 'data_file' in config else None
        self.db_connector = self._get_db_connector()

    def _get_db_connector(self):
        db_target = self.config['database']['target']
        if db_target == 'supabase':
            return SupabaseConnector(
                url=os.environ.get("SUPABASE_URL"),
                key=os.environ.get("SUPABASE_KEY")
            )
        elif db_target == 'clickhouse':
            return ClickHouseConnector(
                host=os.environ.get("CLICKHOUSE_HOST"),
                port=os.environ.get("CLICKHOUSE_PORT"),
                user=os.environ.get("CLICKHOUSE_USER"),
                password=os.environ.get("CLICKHOUSE_PASSWORD"),
            )
        else:
            raise ValueError(f"Unsupported database target: {db_target}")

    def run(self):
        if not self.data_engine:
            print("No data_file provided in config. Skipping data processing.")
            return

        run_id = str(uuid.uuid4())
        start_time = datetime.now().isoformat()
        status = "running"
        error_message = None
        records_processed = 0

        run_data = {
            "run_id": run_id,
            "pipeline_name": "data_ingestion",
            "run_type": "manual", # Assuming manual for now
            "database_target": self.config['database']['target'],
            "start_timestamp": start_time,
            "status": status,
            "config_snapshot": self.config # Log the config used
        }

        self.db_connector.log_pipeline_run_start(run_data)

        try:
            # Get last watermark
            last_watermark = self.db_connector.get_last_watermark(
                pipeline_name="data_ingestion", 
                platform_type="twitter"
            )
            print(f"Last watermark: {last_watermark}")

            # 1. Read data
            df = self.data_engine.read_data(self.config['data_file'], last_watermark)
            if self.data_engine.__class__.__name__ == "SparkDataEngine":
                df_count = df.count()
                print(f"DataFrame size after reading and filtering: {df_count}")
            else:
                print(f"DataFrame size after reading and filtering: {len(df)}")

            # 2. Normalize data
            df = self.data_engine.normalize_data(df)

            # 3. Quality check
            df = self.data_engine.quality_check(df)

            # 4. Get records
            records = self.data_engine.get_records(df)
            if self.data_engine.__class__.__name__ == "SparkDataEngine":
                records_processed = df.count()
            else:
                records_processed = len(records)

            if records_processed > 0:
                print('records length needed to be processed',records_processed)
                # 5. Connect to DB

                self.db_connector.connect()

                # 6. Create tables if they don't exist
                self.db_connector.create_tables()

                # 7. Batch insert
                self.db_connector.batch_insert(records)

                # Update watermark
                if self.data_engine.__class__.__name__ == "SparkDataEngine":
                    latest_timestamp = df.agg({"interaction_timestamp": "max"}).collect()[0][0]
                else:
                    latest_timestamp = df['interaction_timestamp'].max()
                self.db_connector.update_watermark(
                    pipeline_name="data_ingestion",
                    platform_type="twitter",
                    timestamp=latest_timestamp.isoformat(),
                    records_processed=records_processed,
                    status="completed"
                )

            status = "completed"
        except Exception as e:
            status = "failed"
            error_message = str(e)
            print(f"Pipeline run failed: {error_message}")
        finally:
            end_time = datetime.now()
            end_data = {
                "end_timestamp": end_time,
                "status": status,
                "records_processed": records_processed,
                "error_message": error_message
            }
            self.db_connector.log_pipeline_run_end(run_id, end_data)

    def process_nba_data(self):
        # 1. Fetch all interactions
        interactions = self.db_connector.fetch_interactions()

        # 2. Process interactions into conversations and customer profiles
        conversation_processor = ConversationProcessor()
        processed_data = conversation_processor.process_interactions(interactions)

        # 3. Insert into conversations table
        self.db_connector.batch_insert_conversations(processed_data['conversations'])

        # 4. Insert into customer_profiles table
        self.db_connector.batch_insert_customer_profiles(processed_data['customer_profiles'])

    def run_nba_predictions(self, limit_customers=None):
        # Efficient customer sampling (not fetch all!)
        customer_profiles = self.db_connector.fetch_customer_profiles(limit=limit_customers)
        
        nba_engine = NBAEngine(self.db_connector)
        all_customer_data = []
        resolved_customers_count = 0
        
        print(f"Processing NBA predictions for {len(customer_profiles)} customers...")
        
        for i, customer_profile in enumerate(customer_profiles):
            customer_id = customer_profile.get('customer_id')
            
            try:
                conversation_summary = self.db_connector.fetch_customer_conversation(customer_id)
                if not isinstance(conversation_summary, dict):
                    conversation_summary = {}

                if conversation_summary.get('resolution_status') == 'resolved':
                    resolved_customers_count += 1
                    continue # Skip resolved issues

                conversation_history = self.db_connector.fetch_customer_interactions(customer_id)

                prediction = nba_engine.predict_for_customer(customer_id)
                all_customer_data.append({
                    'prediction': prediction,
                    'conversation_summary': conversation_summary,
                    'conversation_history': conversation_history
                })
                
                if (i + 1) % 100 == 0:
                    print(f"Processed {i + 1}/{len(customer_profiles)} customers")
                    
            except Exception as e:
                print(f"Error processing customer {customer_id}: {e}")
                continue
        
        print(f"Skipped {resolved_customers_count} customers with 'resolved' issues.")

        # Export results
        if all_customer_data:
            self.export_predictions_csv(all_customer_data)
            print(f"NBA predictions exported for {len(all_customer_data)} customers")
        
        return all_customer_data

    def export_predictions_csv(self, all_customer_data: List[Dict]):
        if not all_customer_data:
            print("No predictions to export.")
            return

        processed_data = []

        for item in all_customer_data:
            prediction = item['prediction']
            conversation_summary = item.get('conversation_summary', '')
            conversation_history = item.get('conversation_history', [])

            # Generate chat_log
            chat_log = ""
            if conversation_history:
                for interaction in conversation_history:
                    participant = interaction.get('participant_external_id', 'Unknown')
                    content = interaction.get('content_text', '')
                    chat_log += f"{participant}: {content}\n"

            # Determine issue_status
            issue_status = "pending_customer_response"  # Default
            if prediction.get('channel') == 'scheduling_phone_call':
                issue_status = "escalated"
            # Add more rules for issue_status based on your logic

            processed_data.append({
                'customer_id': prediction.get('customer_id'),
                'chat_log': chat_log.strip(),
                'channel': prediction.get('channel'),
                'message': prediction.get('message'),
                'send_time': prediction.get('send_time'),
                'reasoning': prediction.get('reasoning'),
                'issue_status': issue_status
            })

        df = pd.DataFrame(processed_data)
        output_path = "nba_predictions.csv"
        df.to_csv(output_path, index=False)
        print(f"Predictions exported to {output_path}")

    def run_nba_api(self,port:int = 8080):
        """Start FastAPI server for single customer predictions"""
        import uvicorn
        from api.nba_api import create_app
        
        # Pass db_connector config to FastAPI app
        os.environ["DB_TARGET"] = self.config['database']['target']
        app = create_app(self.config['database']['target'])
        uvicorn.run(app, host="0.0.0.0", port=port)



if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Run the Riverline data pipeline.")
    parser.add_argument("--data_file", type=str, default="/home/arvind/personal-projects/riverline/backend/riverline_backend/data/10/twcs/twcs.csv",
                        help="Path to the input CSV data file.")
    parser.add_argument("--db", type=str, default="supabase", choices=["supabase", "clickhouse"],
                        help="Database target: 'supabase' or 'clickhouse'.")
    parser.add_argument("--action", type=str, default="run_pipeline",
                        choices=["run_pipeline", "process_nba_data", "run_nba_predictions", "run_nba_api"],
                        help="Action to perform: 'run_pipeline', 'process_nba_data', 'run_nba_predictions', or 'run_nba_api'.")
    parser.add_argument("--customers", type=int, default=None,
                        help="Limit number of customers for NBA predictions (for batch processing).")

    args = parser.parse_args()

    config = {
        'data_file': args.data_file,
        'database': {
            'target': args.db
        }
    }

    pipeline = Pipeline(config)

    if args.action == "run_pipeline":
        pipeline.run()
    elif args.action == "process_nba_data":
        pipeline.process_nba_data()
    elif args.action == "run_nba_predictions":
        pipeline.run_nba_predictions(limit_customers=args.customers)
    elif args.action == "run_nba_api":
        pipeline.run_nba_api()
