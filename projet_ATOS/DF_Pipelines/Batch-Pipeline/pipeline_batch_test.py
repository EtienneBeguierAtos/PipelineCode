import logging
import unittest
import sys
#import pipeline_batch as pipeline
#import messages
import time
from google.cloud import bigquery
from IPython import embed
import googleapiclient.discovery
from google.appengine import api

def app(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/plain')])
    yield b'Hello world!\n'

app = api.wrap_wsgi_app(app)


sys.argv=["--skipLeadingRows=1"]
project_id = "smartlive"
#client=bigquery.Client()

class TestStringMethods(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client=bigquery.Client()
        cls.reference={
            "clients_count":list(cls.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.age`""").result()),
            "address_count":list(cls.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.address`""").result()),
            "telephone_count":list(cls.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.telephone`""").result()),
            "telephone_rejects_count":list(cls.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneRejects.telephone`""").result()),
            "age_rejects_count":list(cls.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneRejects.age`""").result()),
            "monitoring_count":list(cls.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.monitoring`""").result())
        }
               
        dataflow = googleapiclient.discovery.build('dataflow', 'v1b3')
        start=time.time()
        result = dataflow.projects().locations().templates().launch(
            projectId=project_id,
            body={
                "jobName": "pipeline_batch",
                "parameters": {
                    "fileURL" : "gs://etienne_files/client1.csv",
                    "destination": "telephone",
                    "timestamp": "1111",
                    "id": "000000"
                    },
                "environment": {
                    "tempLocation": "gs://etienne_files/temp",
                    "zone": "europe-west1-b",
                    "machineType": "n1-highcpu-16"
                    },
                },
            gcsPath = 'gs://etienne_files/templates_test/batch_template',
            location="europe-west1",
            #wait_until_finished=True
        ).execute()
        end=time.time()
        logging.info("duration of pipeline: "+str(end-start))      

    def test_clients_count(self):
        clients_count = list(self.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.age`""").result())
        self.assertEqual(clients_count[0].get('f0_'), self.reference["clients_count"][0].get('f0_')+1)
    
    def test_clients_rejects_count(self):
        rejects_count = list(self.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneRejects.age`""").result())
        self.assertEqual(rejects_count[0].get('f0_'), self.reference["age_rejects_count"][0].get('f0_')+1)

    def test_address_count(self):
        address_count = list(self.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.address`""").result())
        self.assertEqual(address_count[0].get('f0_'), self.reference["address_count"][0].get('f0_')+0)

    def test_telephone_count(self):
        telephone_count = list(self.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.telephone`""").result())
        self.assertEqual(telephone_count[0].get('f0_'), self.reference["telephone_count"][0].get('f0_'))

    def test_rejects_count(self):
        telephone_rejects_count = list(self.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneRejects.telephone`""").result())
        self.assertEqual(telephone_rejects_count[0].get('f0_'), self.reference["telephone_rejects_count"][0].get('f0_'))

    def test_monitoring_count(self):
        clients_count = list(self.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.monitoring`""").result())
        self.assertEqual(clients_count[0].get('f0_'), self.reference["monitoring_count"][0].get('f0_')+2)


#exec('pipeline.py')
#subprocess.call(["python3 pipeline.py","--skipLeadingRows=1"])
#pipeline.run_batch('DirectRunner')
#messages.run()
#time.sleep(20)
#messages.run()
#time.sleep(20)
#messages.run()
#time.sleep(20)


unittest.main()

#job=client.query("""TRUNCATE TABLE `smartlive.EtienneResults.age`""")
#job.result()

#address_job = client.query("""SELECT * FROM `smartlive.EtienneResults.address`""")
#clients = client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.etienne12`""").result()
#embed()
#list_clients=list(clients)
#print(list_clients[0].get('f0_'))