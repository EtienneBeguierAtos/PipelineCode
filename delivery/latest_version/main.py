import base64
import json
import os
import logging
import apache_beam
import schema
import pipeline_batch

from google.cloud import pubsub_v1, storage

project_id = "smartlive"
topic_id1 = "my_topic1"

publisher = pubsub_v1.PublisherClient()
storage_client=storage.Client()

def pipeline_trigger(event, context):
    message_string=base64.b64decode(event['data'])
    message=json.loads(base64.b64decode(event['data']).decode('utf-8'))
    url=message["fileURL"]
    splitted_url=url.replace('gs://','').split('/')
    size=storage_client.get_bucket(splitted_url[0]).get_blob(splitted_url[1]).size
    #file_metadata = requests.get('https://storage.googleapis.com/storage/v1/b/etienne_files/o/telephone1.csv/alt/json')
    #size=file_metadata["size"]
    print("file size: "+str(size))
    if size<10**9:
        publisher.publish(publisher.topic_path(project_id, topic_id1), message_string)
    else:    
        pipeline_batch.run_batch(message)


    return "OK"