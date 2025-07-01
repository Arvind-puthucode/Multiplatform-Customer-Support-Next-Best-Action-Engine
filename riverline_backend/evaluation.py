
import pandas as pd
from .nba.nba_engine import NBAEngine
import json
from typing import List, Dict

def run_evaluation(db_connector, num_customers=1000):
    # 1. Fetch customer profiles and interactions
    customer_profiles = db_connector.fetch_customer_profiles()
    interactions = db_connector.fetch_interactions()

    # Limit to num_customers for evaluation if needed
    if num_customers and len(customer_profiles) > num_customers:
        customer_profiles = customer_profiles[:num_customers]

    # Group interactions by customer_id
    customer_interactions = {}
    for interaction in interactions:
        customer_id = interaction.get('participant_external_id')
        if customer_id not in customer_interactions:
            customer_interactions[customer_id] = []
        customer_interactions[customer_id].append(interaction)

    # 2. Initialize the NBA engine
    nba_engine = NBAEngine()

    # 3. Generate NBA outputs for each customer
    nba_outputs = []
    chat_logs = []
    issue_statuses = []

    for customer_profile in customer_profiles:
        customer_id = customer_profile.get('customer_id')
        conversation_history = customer_interactions.get(customer_id, [])

        # Generate NBA prediction
        prediction = nba_engine.predict_action(customer_profile, conversation_history)
        nba_outputs.append(prediction)

        # Construct chat_log
        chat_log_str = ""
        for msg in sorted(conversation_history, key=lambda x: x['interaction_timestamp']):
            chat_log_str += f"Customer: {msg['content_text']}\n" # Assuming all interactions are from customer for simplicity
        chat_logs.append(chat_log_str)

        # Determine issue status based on the action
        if prediction['channel'] == 'scheduling_phone_call':
            issue_statuses.append('escalated')
        elif prediction['channel'] == 'email_reply':
            issue_statuses.append('pending_customer_reply')
        else:
            issue_statuses.append('resolved')

    # 4. Create a DataFrame and export to CSV
    df = pd.DataFrame(nba_outputs)
    df['chat_log'] = chat_logs
    df['issue_status'] = issue_statuses

    # Reorder columns to match the specified format
    df = df[['customer_id', 'channel', 'send_time', 'message', 'reasoning', 'chat_log', 'issue_status']]

    df.to_csv('nba_results.csv', index=False)

    print(f"NBA evaluation results saved to nba_results.csv for {len(customer_profiles)} customers.")

if __name__ == '__main__':
    # This part will be called from main.py, so it needs a db_connector
    # For standalone testing, you would mock a db_connector here.
    pass
