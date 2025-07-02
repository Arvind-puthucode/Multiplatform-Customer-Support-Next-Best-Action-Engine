
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file
import uuid
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

    def run_nba_predictions(self):
        # Fetch all interactions and customer profiles
        interactions = self.db_connector.fetch_interactions()
        customer_profiles = self.db_connector.fetch_customer_profiles() # Need to implement this in SupabaseConnector

        # Group interactions by customer_id
        customer_interactions = {}
        for interaction in interactions:
            customer_id = interaction.get('participant_external_id')
            if customer_id not in customer_interactions:
                customer_interactions[customer_id] = []
            customer_interactions[customer_id].append(interaction)

        nba_engine = NBAEngine()
        predictions = []

        # Generate predictions for each customer
        for customer_profile in customer_profiles:
            customer_id = customer_profile.get('customer_id')
            conversation_history = customer_interactions.get(customer_id, [])
            
            prediction = nba_engine.predict_action(customer_profile, conversation_history)
            predictions.append(prediction)

        # Print predictions (for MVP, later save to CSV)
        for p in predictions:
            print(p)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Run the Riverline data pipeline.")
    parser.add_argument("--data_file", type=str, default="/home/arvind/personal-projects/riverline/backend/riverline_backend/data/10/twcs/twcs.csv",
                        help="Path to the input CSV data file.")
    parser.add_argument("--db", type=str, default="supabase", choices=["supabase", "clickhouse"],
                        help="Database target: 'supabase' or 'clickhouse'.")
    parser.add_argument("--action", type=str, default="run_pipeline",
                        choices=["run_pipeline", "process_nba_data", "run_nba_predictions"],
                        help="Action to perform: 'run_pipeline', 'process_nba_data', or 'run_nba_predictions'.")

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
        pipeline.run_nba_predictions()
