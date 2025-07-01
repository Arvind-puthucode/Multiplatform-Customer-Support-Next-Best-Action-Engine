import pandas as pd
from typing import List, Dict
from datetime import datetime, timedelta, timezone

class NBAEngine:
    def __init__(self):
        pass

    def predict_action(self, customer_profile: Dict, conversation_history: List[Dict]) -> Dict:
        # For MVP, simple rule-based logic
        # In a real system, this would involve more sophisticated ML models

        customer_id = customer_profile.get('customer_id', 'unknown')
        
        # Sort conversation history by timestamp
        conversation_history = sorted(conversation_history, key=lambda x: x['interaction_timestamp'])
        
        latest_interaction_time = None
        if conversation_history:
            latest_interaction_time = pd.to_datetime(conversation_history[-1]['interaction_timestamp'])

        # Default action
        channel = "twitter_dm_reply"
        message = f"Hi {customer_id}, we're looking into your issue. We'll get back to you shortly."
        reasoning = "Default action for open issues."
        send_time = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat() # 1 hour from now

        # Rule 1: If conversation is long (more than 5 interactions), suggest phone call
        if len(conversation_history) > 5:
            channel = "scheduling_phone_call"
            message = f"Hi {customer_id}, your issue seems complex. Let's schedule a call to resolve this quickly."
            reasoning = "Conversation is lengthy, suggesting a direct phone call for efficient resolution."
            send_time = (datetime.now() + timedelta(hours=2)).isoformat() # 2 hours from now

        # Rule 2: If last interaction was more than 24 hours ago, suggest email reply
        if latest_interaction_time and (datetime.now(timezone.utc) - latest_interaction_time) > timedelta(hours=24):
            channel = "email_reply"
            message = f"Hi {customer_id}, we noticed your issue is still open. Please reply to this email with an update."
            reasoning = "Issue has been open for over 24 hours, prompting an email follow-up."
            send_time = (datetime.now() + timedelta(minutes=30)).isoformat() # 30 minutes from now

        # Rule 3: Placeholder for sentiment-based action (requires sentiment analysis)
        # if customer_profile.get('customer_sentiment_score', 0) < -0.5: # Example: very negative sentiment
        #     channel = "scheduling_phone_call"
        #     message = f"Hi {customer_id}, we understand your frustration. Let's connect directly to address your concerns."
        #     reasoning = "Customer sentiment is negative, prioritizing direct communication."

        return {
            "customer_id": customer_id,
            "channel": channel,
            "send_time": send_time,
            "message": message,
            "reasoning": reasoning
        }