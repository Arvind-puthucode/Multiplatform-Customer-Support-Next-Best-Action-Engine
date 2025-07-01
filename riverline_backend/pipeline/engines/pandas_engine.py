
import pandas as pd
from typing import List, Dict, Optional
import uuid

from .base_engine import BaseDataEngine

class PandasDataEngine(BaseDataEngine):
    def __init__(self):
        pass

    def read_data(self, file_path: str, last_processed_timestamp: Optional[str] = None) -> pd.DataFrame:
        try:
            df = pd.read_csv(file_path, encoding='latin-1')
            if last_processed_timestamp:
                print(f"Filtering data with last_processed_timestamp: {last_processed_timestamp}")
                df['created_at'] = pd.to_datetime(df['created_at'], format="%a %b %d %H:%M:%S %z %Y", utc=True)
                df = df[df['created_at'] > pd.to_datetime(last_processed_timestamp, utc=True)]
                print(f"DataFrame size after filtering in read_data: {len(df)}")
            return df
        except FileNotFoundError:
            raise Exception(f"File not found at {file_path}")

    def normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df['interaction_timestamp'] = pd.to_datetime(df['created_at'], format="%a %b %d %H:%M:%S %z %Y", utc=True).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df['platform_type'] = 'twitter'
        df['interaction_type'] = 'tweet'
        df['interaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df_normalized = df.rename(columns={
            'tweet_id': 'external_id',
            'author_id': 'participant_external_id',
            'text': 'content_text',
        })

        # Select and reorder columns to match UniversalInteraction schema
        df_normalized = df_normalized[[
            'interaction_id',
            'external_id',
            'platform_type',
            'participant_external_id',
            'content_text',
            'interaction_timestamp',
            'interaction_type',
        ]]
        return df_normalized

    def quality_check(self, df: pd.DataFrame) -> pd.DataFrame:
        # Drop duplicates based on external_id
        df = df.drop_duplicates(subset=['external_id'])

        # Drop rows with missing content_text
        df = df.dropna(subset=['content_text'])

        # Basic text cleaning
        df['content_text'] = df['content_text'].str.replace(r'http\S+', '', case=False, regex=True)
        df['content_text'] = df['content_text'].str.replace(r'@\S+', '', case=False, regex=True)
        df['content_text'] = df['content_text'].str.replace(r'#\S+', '', case=False, regex=True)

        return df

    def get_records(self, df: pd.DataFrame) -> List[Dict]:
        return df.to_dict('records')
