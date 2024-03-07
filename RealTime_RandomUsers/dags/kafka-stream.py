from datetime import datetime
from airflow import  DAG
from airflow.operators.python import PythonOperator
import uuid 

default_args = {
    'owner': 'poojasingh',
    'start_date': datetime(2024, 3, 1, 12, 00)
}

def get_data():
    import requests 

    response = requests.get("https://randomuser.me/api/")
    response = response.json()
    response = response['results'][0]
    return response


def format_data(response):
    data = {}
    data['id'] = str(uuid.uuid4())
    location = response['location']
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']

    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time 
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    current_time = time.time()

    while True:
        if time.time() > current_time + 60:
            break

        try:
            response = get_data()
            response = format_data(response)
            
            print(json.dumps(response, indent=3))
            producer.send('users_created', json.dumps(response).encode('utf-8'))

        except Exception as e:
            logging.error(f"An error occured : {e}")
            continue


with DAG('uses_automation', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    stream_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable=stream_data
    )
    
# stream_data()