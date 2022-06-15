import unittest
import sys
import pipeline
import messages
import time
from google.cloud import bigquery
from IPython import embed


sys.argv=["--skipLeadingRows=1"]

class TestStringMethods(unittest.TestCase):

    def test_clients_count(self):
        clients_count = list(client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.age`""").result())
        self.assertEqual(clients_count[0].get('f0_'), 2)

    def test_address_count(self):
        address_count = list(client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.address`""").result())
        self.assertEqual(address_count[0].get('f0_'), 1)

    def test_telephone_count(self):
        telephone_count = list(client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.telephone`""").result())
        self.assertEqual(telephone_count[0].get('f0_'), 2)

    def test_rejects_count(self):
        address_rejects_count = list(client.query("""SELECT COUNT(*) FROM `smartlive.EtienneRejects.address`""").result())
        self.assertEqual(address_rejects_count[0].get('f0_'), 1)



#exec('pipeline.py')
#subprocess.call(["python3 pipeline.py","--skipLeadingRows=1"])

pipeline.run()
time.sleep(10)
messages.run()
#time.sleep(20)
#messages.run()
#time.sleep(20)
#messages.run()
time.sleep(10)

"""
x = threading.Thread(target=pipeline2.run)
x.start()

time.sleep(10)
y = threading.Thread(target=messages.run)
y.start()

time.sleep(20)
"""

client = bigquery.Client()

unittest.main()

client.query("""TRUNCATE TABLE `smartlive.EtienneResults.age`""")

#address_job = client.query("""SELECT * FROM `smartlive.EtienneResults.address`""")
#clients = client.query("""SELECT COUNT(*) FROM `smartlive.EtienneResults.etienne12`""").result()
#embed()
#list_clients=list(clients)
#print(list_clients[0].get('f0_'))