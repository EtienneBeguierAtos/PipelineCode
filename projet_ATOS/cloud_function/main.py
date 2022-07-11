import base64
import json
import os
import logging
import apache_beam
import schema
#import pipeline_batch
from oauth2client.client import GoogleCredentials
import googleapiclient.discovery


from google.cloud import pubsub_v1, storage

project_id = "smartlive"
topic_id = "my_topic1"

machine_type=os.environ.get('machine_type', 'n1-standard-4')
max_num_workers=os.environ.get('max_num_workers', 100)

publisher = pubsub_v1.PublisherClient()
storage_client=storage.Client()
credentials = GoogleCredentials.get_application_default()



def pipeline_trigger(event, context):
    message_string=base64.b64decode(event['data'])
    message=json.loads(message_string.decode('utf-8'))
    url=message["fileURL"]
    print("context: "+ str(context))
    splitted_url=url.replace('gs://','').split('/')
    size=storage_client.get_bucket(splitted_url[0]).get_blob(splitted_url[1]).size
    #file_metadata = requests.get('https://storage.googleapis.com/storage/v1/b/etienne_files/o/telephone1.csv/alt/json')
    #size=file_metadata["size"]
    print("file size: "+str(size))
    if size<10**9:
        publisher.publish(publisher.topic_path(project_id, topic_id), message_string)
    else:   
        dataflow = googleapiclient.discovery.build('dataflow', 'v1b3', credentials=credentials)
        result = dataflow.projects().locations().templates().launch(
            projectId=project_id,
            body={
                "jobName": "pipeline_batch",
                "parameters": {
                    "fileURL" : message["fileURL"],
                    "destination": message["destination"],
                    "timestamp": context.timestamp,
                    "id": context.event_id
                    },
                "environment": {
                    "tempLocation": "gs://etienne_files/temp",
                    "zone": "europe-west1-b",
                    "machineType": machine_type,
                    "maxWorkers": max_num_workers
                    },
                },
            gcsPath = 'gs://etienne_files/templates/batch_template',
            location="europe-west1"
        ).execute()


    return "OK"