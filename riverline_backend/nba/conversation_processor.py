
import pandas as pd
from typing import List, Dict
import uuid

class ConversationProcessor:
    def __init__(self):
        pass

    def process_interactions(self, interactions: List[Dict]) -> Dict[str, List[Dict]]:
        df = pd.DataFrame(interactions)

        # Convert timestamp to datetime objects for easier manipulation
        df['interaction_timestamp'] = pd.to_datetime(df['interaction_timestamp'])

        # Sort by customer_id and timestamp to ensure correct conversation flow
        df = df.sort_values(by=['participant_external_id', 'interaction_timestamp'])

        conversations_data = []
        customer_profiles_data = []

        # Group by customer_id to process conversations
        for customer_id, group in df.groupby('participant_external_id'):
            # Conversation aggregation
            conversation_id = str(uuid.uuid4())
            conversation_start_timestamp = group['interaction_timestamp'].min()
            conversation_end_timestamp = group['interaction_timestamp'].max()
            total_interactions = len(group)
            
            # For hackathon MVP, set default values for resolution_status, sentiment, topic, channel_mix
            # These would be determined by more sophisticated ML models in a full system
            resolution_status = 'open'
            customer_sentiment_score = 0.0 # Neutral
            conversation_topic = 'general_inquiry'
            channel_mix = {'twitter': True} # Assuming all from twitter for now

            conversations_data.append({
                'conversation_id': conversation_id,
                'customer_id': customer_id,
                'brand_account_id': 'Riverline', # Placeholder
                'conversation_start_timestamp': conversation_start_timestamp.isoformat(),
                'conversation_end_timestamp': conversation_end_timestamp.isoformat(),
                'total_interactions': total_interactions,
                'resolution_status': resolution_status,
                'customer_sentiment_score': customer_sentiment_score,
                'conversation_topic': conversation_topic,
                'channel_mix': channel_mix,
                'created_at': pd.Timestamp.now(tz='UTC').isoformat()
            })

            # Customer profile aggregation
            total_conversations = 1 # For this single conversation
            avg_response_time = 0.0 # Placeholder
            resolution_rate = 0.0 # Placeholder
            last_interaction_timestamp = conversation_end_timestamp.isoformat()
            
            customer_profiles_data.append({
                'customer_id': customer_id,
                'platform_accounts': {'twitter_id': customer_id}, # Placeholder
                'interaction_history_summary': {'total_interactions': total_interactions}, # Placeholder
                'behavioral_tags': {'default': True}, # Placeholder
                'preferred_channels': {'twitter': True}, # Placeholder
                'avg_response_time': avg_response_time,
                'total_conversations': total_conversations,
                'resolution_rate': resolution_rate,
                'last_interaction_timestamp': last_interaction_timestamp,
                'created_at': pd.Timestamp.now(tz='UTC').isoformat()
            })
        
        return {
            'conversations': conversations_data,
            'customer_profiles': customer_profiles_data
        }
