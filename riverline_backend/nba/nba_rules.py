from typing import Dict, List
from datetime import datetime, timedelta
import re

def calculate_hours_ago(timestamp_str: str) -> float:
    """Calculates hours since a given timestamp string."""
    if not timestamp_str:
        return 0.0
    try:
        # Handle both ISO format and ClickHouse DateTime64 format
        if 'T' in timestamp_str:
            dt_object = datetime.fromisoformat(timestamp_str)
        else:
            dt_object = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
        
        time_difference = datetime.now() - dt_object
        return time_difference.total_seconds() / 3600
    except ValueError:
        return 0.0

def count_interactions_last_24h(conversation_history: List[Dict]) -> int:
    """Counts interactions within the last 24 hours."""
    count = 0
    for interaction in conversation_history:
        if 'interaction_timestamp' in interaction and calculate_hours_ago(interaction['interaction_timestamp']) <= 24:
            count += 1
    return count

def count_conversation_turns(interactions_data: List[Dict]) -> int:
    """Counts the number of turns in a conversation."""
    return len(interactions_data)

def hours_since_last_message(interactions_data: List[Dict]) -> float:
    """Calculates hours since the last message in interactions data."""
    if not interactions_data:
        return 0.0
    last_interaction = max(interactions_data, key=lambda x: x.get('interaction_timestamp', ''))
    return calculate_hours_ago(last_interaction.get('interaction_timestamp', ''))

def calculate_avg_message_length(interactions_data: List[Dict]) -> float:
    """Calculates the average length of messages."""
    if not interactions_data:
        return 0.0
    total_length = sum(len(i.get('content_text', '')) for i in interactions_data)
    return total_length / len(interactions_data)

def check_urgent_words(interactions_data: List[Dict]) -> bool:
    """Checks for urgent keywords in messages."""
    urgent_keywords = ["urgent", "asap", "immediately", "now", "help"]
    for interaction in interactions_data:
        if any(keyword in interaction.get('content_text', '').lower() for keyword in urgent_keywords):
            return True
    return False

def check_escalation_words(interactions_data: List[Dict]) -> bool:
    """Checks for escalation keywords in messages."""
    escalation_keywords = ["escalate", "manager", "complaint", "unacceptable"]
    for interaction in interactions_data:
        if any(keyword in interaction.get('content_text', '').lower() for keyword in escalation_keywords):
            return True
    return False

def analyze_response_pattern(interactions_data: List[Dict]) -> str:
    """Analyzes the response pattern (simple)."""
    # This is a very basic example, can be expanded
    if len(interactions_data) > 1:
        first_interaction_time = interactions_data[0].get('interaction_timestamp', '')
        last_interaction_time = interactions_data[-1].get('interaction_timestamp', '')
        if first_interaction_time and last_interaction_time:
            try:
                first_dt = datetime.fromisoformat(first_interaction_time)
                last_dt = datetime.fromisoformat(last_interaction_time)
                if (last_dt - first_dt).total_seconds() > 3600: # More than an hour
                    return "spread_out"
            except ValueError:
                pass
    return "clustered"

def determine_channel_and_timing(customer_profile: Dict, conversation_history: List[Dict]) -> Dict:
    """Determines the best channel and timing based on rules."""
    # Rule 1: Long conversations → Phone Call
    if len(conversation_history) > 5:
        return {
            'channel': 'scheduling_phone_call',
            'reasoning': f'Conversation has {len(conversation_history)} interactions - phone call more efficient'
        }
    
    # Rule 2: Old unresolved issue → Email followup  
    if conversation_history:
        last_interaction = max(conversation_history, key=lambda x: x.get('interaction_timestamp', ''))
        hours_since_last = calculate_hours_ago(last_interaction.get('interaction_timestamp', ''))
        
        if hours_since_last > 24:
            return {
                'channel': 'email_reply',
                'reasoning': f'Issue dormant for {hours_since_last:.1f} hours - email follow-up needed'
            }
    
    # Rule 3: High interaction frequency → Twitter reply (customer is active)
    recent_interactions = count_interactions_last_24h(conversation_history)
    if recent_interactions >= 3:
        return {
            'channel': 'twitter_dm_reply', 
            'reasoning': f'Customer very active ({recent_interactions} recent interactions) - respond on Twitter'
        }
    
    # Default
    return {
        'channel': 'twitter_dm_reply',
        'reasoning': 'Standard response for active issue'
    }

def extract_simple_features(customer_id: str, interactions_data: List[Dict]) -> Dict:
    """Extracts basic features from interactions data."""
    return {
        'total_interactions': len(interactions_data),
        'conversation_length': count_conversation_turns(interactions_data),
        'hours_since_last_interaction': hours_since_last_message(interactions_data),
        'avg_message_length': calculate_avg_message_length(interactions_data),
        'contains_urgent_keywords': check_urgent_words(interactions_data),
        'has_escalation_words': check_escalation_words(interactions_data),
        'response_pattern': analyze_response_pattern(interactions_data)
    }
