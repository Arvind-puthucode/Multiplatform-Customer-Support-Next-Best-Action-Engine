from typing import List, Dict
from decimal import Decimal
from datetime import datetime, timedelta, timezone



from .nba_rules import determine_channel_and_timing, extract_simple_features
from pipeline.connectors.supabase_connector import SupabaseConnector
from pipeline.connectors.clickhouse_connector import ClickHouseConnector
import os
from openai import AzureOpenAI
from openai.types import ResponseFormatJSONObject
import json
import re

class NBAEngine:
    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.openai_client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version="2024-02-01",
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )

    def _convert_decimals_to_floats(self, obj):
        if isinstance(obj, dict):
            return {k: self._convert_decimals_to_floats(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_decimals_to_floats(elem) for elem in obj]
        elif isinstance(obj, Decimal):
            return float(obj)
        return obj

    def _enhance_with_llm(self, rule_output: Dict, conversation_context: Dict) -> Dict:
        # Convert any Decimal objects in conversation_context to floats
        processed_conversation_context = self._convert_decimals_to_floats(conversation_context)
        prompt = f"""
        Based on this customer support conversation, improve the message and reasoning.
        
        Conversation Summary: {processed_conversation_context.get('summary', '')}
        Rule-based Decision: {rule_output['channel']} - {rule_output['reasoning']}
        
        Generate:
        1. A more personalized message for the customer.
        2. Enhanced reasoning explaining why this channel/timing is best.
        
        Keep the same channel choice. Be concise and helpful.
        
        Respond ONLY with a JSON object containing two keys: "message" and "reasoning".
        Example:
        {{
            "message": "Your personalized message here.",
            "reasoning": "Your enhanced reasoning here."
        }}
        """
        
        response = self.openai_client.chat.completions.create(
            model=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
            messages=[{"role": "user", "content": prompt}],
            response_format=ResponseFormatJSONObject(type="json_object")
        )
            # response_format={{ "type": "json_object" }}
        llm_response_content = response.choices[0].message.content

        try:
            parsed_response = json.loads(llm_response_content)
            enhanced_message = parsed_response.get("message", "")
            enhanced_reasoning = parsed_response.get("reasoning", "")
        except json.JSONDecodeError:
            print("Warning: LLM did not return valid JSON. Falling back to rule output.")
            enhanced_message = rule_output['reasoning'] # Fallback if JSON parsing fails
            enhanced_reasoning = rule_output['reasoning']
        

        return {
            "message": enhanced_message,
            "reasoning": enhanced_reasoning
        }

    def predict_for_customer(self, customer_id: str) -> Dict:
        customer_profile = self.db_connector.fetch_customer_profile(customer_id)
        if not customer_profile:
            raise ValueError(f"Customer profile not found for ID: {customer_id}")

        conversation_history = self.db_connector.fetch_customer_interactions(customer_id)
        conversation_summary = self.db_connector.fetch_customer_conversation(customer_id)
        if not isinstance(conversation_summary, dict):
            conversation_summary = {} # Ensure it's a dictionary
        print('data fetch done for customer ', customer_id)
        # 1. Feature Extraction
        features = extract_simple_features(customer_id, conversation_history)

        # 2. Rule-based Decision
        rule_output = determine_channel_and_timing(customer_profile, conversation_history)

        # 3. LLM Enhancement
        enhanced_output = self._enhance_with_llm(rule_output, conversation_summary)

        # Combine results
        prediction = {
            "customer_id": customer_id,
            "channel": rule_output['channel'],
            "send_time": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(), # Placeholder for dynamic send time
            "message": enhanced_output['message'],
            "reasoning": enhanced_output['reasoning']
        }
        return prediction