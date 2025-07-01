import argparse
import os
from riverline_backend.pipeline.main import Pipeline
from riverline_backend.evaluation import run_evaluation
from riverline_backend.pipeline.connectors.supabase_connector import SupabaseConnector

from dotenv import load_dotenv
load_dotenv()

def main():
    parser = argparse.ArgumentParser(description='Riverline NBA Project')
    parser.add_argument('action', choices=['run_pipeline', 'run_evaluation', 'process_nba_data', 'run_nba_predictions'], help='Action to perform')
    parser.add_argument('--db', choices=['supabase', 'clickhouse'], default='supabase', help='Database to use')
    args = parser.parse_args()

    if args.action == 'run_pipeline':
        config = {
            'data_file': '/home/arvind/personal-projects/riverline/backend/riverline_backend/data/10/sample.csv',
            'database': {
                'target': args.db
            }
        }
        pipeline = Pipeline(config)
        pipeline.run()
    elif args.action == 'run_evaluation':
        config = {
            'database': {
                'target': args.db
            }
        }
        pipeline = Pipeline(config)
        run_evaluation(pipeline.db_connector)
    elif args.action == 'process_nba_data':
        config = {
            'database': {
                'target': args.db
            }
        }
        pipeline = Pipeline(config)
        pipeline.process_nba_data()
    elif args.action == 'run_nba_predictions':
        config = {
            'database': {
                'target': args.db
            }
        }
        pipeline = Pipeline(config)
        pipeline.run_nba_predictions()

if __name__ == '__main__':
    main()