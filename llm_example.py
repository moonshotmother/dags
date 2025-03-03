# SPDX-License-Identifier: Apache-2.0
from datetime import datetime, timedelta
from enum import Enum

from airflow import DAG
from airflow.operators.python import PythonOperator
from openai import BadRequestError, OpenAI
from pydantic import BaseModel

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'guided_decoding_workflow',
    default_args=default_args,
    description='A DAG to perform guided decoding using LLM API',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 3, 3),
    catchup=False,
    tags=['llm', 'guided_decoding'],
)

# Function to perform sentiment classification using guided choice
def classify_sentiment(**kwargs):
    client = OpenAI(
        base_url="http://localhost:11434/v1",  # ollama
        # base_url="http://localhost:8000/v1",  # vllm
        api_key="-",
    )
    model = "deepseek-r1:32b"
    
    completion = client.chat.completions.create(
        model=model,
        messages=[{
            "role": "user",
            "content": "Classify this sentiment: vLLM is wonderful!"
        }],
        extra_body={"guided_choice": ["positive", "negative"]},
    )
    
    result = completion.choices[0].message.content
    print(f"Sentiment classification result: {result}")
    return result

# Function to generate email using guided regex
def generate_email(**kwargs):
    client = OpenAI(
        base_url="http://localhost:11434/v1",  # ollama
        # base_url="http://localhost:8000/v1",  # vllm
        api_key="-",
    )
    model = "deepseek-r1:32b"
    
    prompt = ("Generate an email address for Alan Turing, who works in Enigma."
              "End in .com and new line. Example result:"
              "alan.turing@enigma.com\n")
    
    completion = client.chat.completions.create(
        model=model,
        messages=[{
            "role": "user",
            "content": prompt,
        }],
        extra_body={
            "guided_regex": r"\w+@\w+\.com\n",
            "stop": ["\n"]
        },
    )
    
    result = completion.choices[0].message.content
    print(f"Generated email: {result}")
    return result

# Define the tasks
sentiment_task = PythonOperator(
    task_id='classify_sentiment',
    python_callable=classify_sentiment,
    provide_context=True,
    dag=dag,
)

email_task = PythonOperator(
    task_id='generate_email',
    python_callable=generate_email,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
sentiment_task >> email_task