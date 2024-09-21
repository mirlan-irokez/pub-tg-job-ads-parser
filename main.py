from flask import Flask, request, jsonify
from telethon import TelegramClient
import re
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
import numpy as np
import os

app = Flask(__name__)

# Telegram Authorization
api_id = 'YOUR_API_ID'
api_hash = 'YOUR_API_HASH'
client_tg = TelegramClient('session_name', api_id, api_hash)

# Channel starting with @
channel_name = '@TG_CHANNEL_TO_PARSE'

# Define Google Auth, dataset, table in BigQuery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'YOUR_AUTH_CREDENTIALS'
client_bq = bigquery.Client()
dataset_id = 'YOUR_BIGQUERY_DATASET_NAME'
table_id = 'YOUR_BIGQUERY_TABLE_NAME'

# Function to fetch last 50 messages from Telegram channel
def fetch_messages(channel):
    with client_tg:
        print('Starting client ...')
        client_tg.start()
        print('Client started')
        messages = []
        for message in client_tg.iter_messages(channel, limit=50):
            messages.append({
                'id': message.id,
                'datetime': message.date,
                'sender': message.sender_id,
                'text': message.text
            })
        print('Messages fetched')
        return messages

# Parser. This function is an example for job ad message type
def parse_ad_job(text):
    parsed_ad = {
        'company': None,
        'position': None,
        'job_type': None,
        'salary': None,
        'currency': None,
        'time_period': None,
        'link': None
    }

    # Company and position
    company_position_pattern = r'^([^\n:]+):([^\n:]+)'
    company_position_match = re.search(company_position_pattern, text)
    if company_position_match:
        parsed_ad['company'] = company_position_match.group(1).strip().replace('**', '')
        parsed_ad['position'] = company_position_match.group(2).strip().replace('**', '')
    else:
        parsed_ad['company'] = None
        parsed_ad['position'] = None

    # Job type
    job_type_pattern = r'Type: (.*)'
    job_type_match = re.search(job_type_pattern, text)
    if job_type_match:
        parsed_ad['job_type'] = job_type_match.group(1).strip()
    else:
        parsed_ad['job_type'] = None

    # Salary and currency
    salary_currency_pattern = r'(?:From\s)?(\d+ - \d+|\d+)\s(USD|EUR)\s(в\s(month|hour))'
    salary_currency_match = re.search(salary_currency_pattern, text)
    if salary_currency_match:
        parsed_ad['salary'] = salary_currency_match.group(1).strip()
        parsed_ad['currency'] = salary_currency_match.group(2).strip()
        parsed_ad['time_period'] = salary_currency_match.group(4).strip()
    else:
        # Skip this ad if the salary pattern does not match
        parsed_ad['salary'] = None
        parsed_ad['currency'] = None
        parsed_ad['time_period'] = None

    # Link
    link_pattern = r'(https://\S+)'
    link_match = re.search(link_pattern, text)
    if link_match:
        parsed_ad['link'] = link_match.group(1).strip()
    else:
        parsed_ad['link'] = None

    return parsed_ad

# Load to BigQuery
def load_to_bq(dataframe):
    # Define BigQuery table schema
    schema = [
        bigquery.SchemaField('id', 'INT64'),
        bigquery.SchemaField('datetime', 'TIMESTAMP'),
        bigquery.SchemaField('sender', 'INT64'),
        bigquery.SchemaField('text', 'STRING'),
        bigquery.SchemaField('company', 'STRING'),
        bigquery.SchemaField('position', 'STRING'),
        bigquery.SchemaField('job_type', 'STRING'),
        bigquery.SchemaField('salary', 'STRING'),
        bigquery.SchemaField('currency', 'STRING'),
        bigquery.SchemaField('time_period', 'STRING'),
        bigquery.SchemaField('link', 'STRING'),
        bigquery.SchemaField('max_salary', 'INT64')
    ]

    # Loading dataframe to BigQuery table by chunks
    chunk_size = 1000
    for i in range(0, len(dataframe), chunk_size):
        chunk = dataframe[i:i + chunk_size]  # Get a chunk of data

        # Convert the chunk to a list of dictionaries for insertion
        rows_to_insert = chunk.to_dict(orient='records')
        # Create a job configuration
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        # Start the job
        try:
            table_ref = client_bq.dataset(dataset_id).table(table_id)
            table = client_bq.get_table(table_ref)
            errors = client_bq.insert_rows(table, rows_to_insert)

            if not errors:
                print(f"Batch {i // chunk_size + 1} inserted successfully.")
            else:
                print(f"Encountered errors while inserting batch {i // chunk_size + 1}:")
                print(errors)

        except Exception as e:
            print(f"Error: {e}")
    return "Data processing and upload completed. Check logs above"

@app.route('/')
def run_parser():
    # Step1: Fetch messages
    messages = fetch_messages(channel=channel_name)

    # Step 2: Create Dataframe and parse Data
    df = pd.DataFrame(messages)
    # Subset only messages from last day
    yesterday = datetime.strftime(datetime.today() - timedelta(1), '%Y-%m-%d')
    df_yesterday = df[df['datetime'].dt.strftime('%Y-%m-%d') == yesterday].reset_index(drop=True)

    # Parse text column
    parse_text = df_yesterday['text'].apply(parse_ad_job)
    parsed_df = pd.DataFrame(parse_text.tolist())

    # Parse salary column to get max value
    parsed_df['max_salary'] = parsed_df['salary'].apply(
        lambda x: int(x.split(' - ')[1]) if x and ' - ' in x else (int(x) if x else None)
    )
    parsed_df['max_salary'] = parsed_df['max_salary'].replace({np.nan: None})

    # Concat parsed data in final dataframe
    global df_final
    df_final = pd.concat([df_yesterday, parsed_df], axis=1)

    # Step 3: Load to BQ
    return load_to_bq(dataframe=df_final)

if __name__ =='__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)