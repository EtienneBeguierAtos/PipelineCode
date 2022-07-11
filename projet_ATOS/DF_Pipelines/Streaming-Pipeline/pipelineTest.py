import logging
import unittest
import sys
#import pipeline_streaming as pipeline
import messages
import time
from google.cloud import bigquery
from IPython import embed
import os
import signal
from subprocess import Popen, PIPE
from multiprocessing import Process


sys.argv=["--skipLeadingRows=1"]

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
        messages.run()
        time.sleep(60)
        #process.kill()

    def test_clients_count(self):
        clients_count = list(self.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.age`""").result())
        self.assertEqual(clients_count[0].get('f0_'), self.reference["clients_count"][0].get('f0_')+1)

    def test_clients_rejects_count(self):
        rejects_count = list(self.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneRejects.age`""").result())
        self.assertEqual(rejects_count[0].get('f0_'), self.reference["age_rejects_count"][0].get('f0_')+1)

    def test_address_count(self):
        address_count = list(self.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.address`""").result())
        self.assertEqual(address_count[0].get('f0_'), self.reference["address_count"][0].get('f0_')+2)

    def test_telephone_count(self):
        telephone_count = list(self.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.telephone`""").result())
        self.assertEqual(telephone_count[0].get('f0_'), self.reference["telephone_count"][0].get('f0_')+1)

    def test_rejects_count(self):
        telephone_rejects_count = list(self.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneRejects.telephone`""").result())
        self.assertEqual(telephone_rejects_count[0].get('f0_'), self.reference["telephone_rejects_count"][0].get('f0_')+1)

    def test_monitoring_count(self):
        clients_count = list(self.client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.monitoring`""").result())
        self.assertEqual(clients_count[0].get('f0_'), self.reference["monitoring_count"][0].get('f0_')+6)


#exec('pipeline.py')
#subprocess.call(["python3 pipeline.py","--skipLeadingRows=1"])
start=time.time()
#process=Popen('exec '+'bash deploy.sh --runner=DirectRunner', shell=True, stdin=PIPE, stdout=PIPE)
#pipeline.run_streaming('DirectRunner')
end=time.time()
duration=end-start
logging.info("duration of pipeline: "+str(duration))
#time.sleep(10)
#messages.run()
#time.sleep(20)
#messages.run()
#time.sleep(20)
#messages.run()
#time.sleep(20)

"""
x = threading.Thread(target=pipeline2.run)
x.start()

time.sleep(10)
y = threading.Thread(target=messages.run)
y.start()

time.sleep(20)
"""


unittest.main()
time.sleep(20)
#process.wait()

#job=client.query("""TRUNCATE TABLE `smartlive.EtienneResults.age`""")
#job.result()

#address_job = client.query("""SELECT * FROM `smartlive.EtienneResults.address`""")
#clients = client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.etienne12`""").result()
#embed()
#list_clients=list(clients)
#print(list_clients[0].get('f0_'))