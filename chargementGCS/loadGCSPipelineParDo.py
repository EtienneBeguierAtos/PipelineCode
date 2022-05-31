import argparse
import io
import time
import logging
import json
import typing
from datetime import datetime
from constant import tables, table_schema2, table_schema
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AfterProcessingTime, AfterAll, AfterAny
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.runners import DataflowRunner, DirectRunner
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
#from IPython import embed

# Table schema for BigQuery
"""table_schema = {
    'fields':[
        {
            "name": "name",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "age",
            "mode": "NULLABLE",
            "type": "INTEGER"
        },
        {
            "name": "height",
            "mode": "NULLABLE",
            "type": "FLOAT"
        }
    ] 
}"""
# ### functions and classes

def getSchema(element):
    names=element["address"].split("/")
    if not names[-1] in tables:
        return tables["tabledata.csv"]
    return tables[names[-1]]


def getMessage(filename, messages):
    for message in messages:
        if message["address"].equals(filename):
            return message
    return


def parse_json(element):
    row = json.loads(element.decode('utf-8'))
    return row #CommonLog(**row)

def partition_fn(elements, num):
    for element in elements:
        return element


def getURI(element):
    return element["address"]

def getDest(element):
    return element["destination"]

def cross_join(left, rights):
    return [left, rights]

class GetTimestamp(beam.DoFn):
  def process(self, plant, timestamp=beam.DoFn.TimestampParam):
    event_id = int(timestamp.micros / 1e6)

dictTypes={
    'int64':'INTEGER',
    'object':'STRING',
    'float64':'FLOAT'
}

def dataframeToSchema(dataframe):
    schema=[]
    for column in dataframe.columns:
        schema.append({
            'name':column,
            "mode": "NULLABLE",
            'type':dictTypes[str(dataframe.dtypes[column])]
            }
        )
    return schema


class ReadCsv(beam.DoFn):
    def start_bundle(self):
        self.storage_client = storage.Client()
    
    def process(self, message):
        address=message["address"].split("/")
        bucket = self.storage_client.get_bucket(address[2])
        blob_address=address[3]
        for i in range(4, len(address)):
            blob_address+="/"+address[i]
        blob = bucket.get_blob(blob_address)
        downloaded_blob = blob.download_as_string().decode('utf-8')
        buffer=io.StringIO(downloaded_blob)
        out = pd.read_csv(filepath_or_buffer = buffer)
        lines=downloaded_blob.split("\r\n")

        line_count = 0
        schema=dataframeToSchema(out)
        schemal=[]
        #embed()
        list_dict=[]
        for line in lines:
            dict={}
            splitted_line=line.split(",")
            if line_count==0:
                schemal=splitted_line
                line_count+=1
            else:
                for col in range(len(schemal)):
                    dict[schemal[col]]=splitted_line[col]
                list_dict.append(dict)
        yield [list_dict,message,schema]




# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into a dynamic BigQuery output')


    # Setting up the Beam pipeline options
    options = PipelineOptions( save_main_session=True, streaming=True)
    options.view_as(GoogleCloudOptions).project = 'smartlive'
    options.view_as(GoogleCloudOptions).region = 'europe-west1'
    options.view_as(GoogleCloudOptions).staging_location = 'gs://testinsertbigquery/staging'
    options.view_as(GoogleCloudOptions).temp_location = 'gs://testinsertbigquery/temp'
    options.view_as(GoogleCloudOptions).dataflow_service_options=["enable_prime"]
    options.view_as(DebugOptions).experiments=["use_runner_v2"]
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('chargement-fichier-streaming-primerunnerv2',time.time_ns())
    options.view_as(SetupOptions).requirements_file="requirements.txt"
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    input_topic = 'projects/smartlive/topics/my_topic3'

    dataset='EtienneResults'


    

    def table_fn(element):
        destination=element["destination"]
        return options.view_as(GoogleCloudOptions).project + "." + dataset + "."+ destination


    

    class InsertCsv(beam.DoFn):
        def start_bundle(self):
            self.bigquery_client = bigquery.Client()

        def process(self, pair):
            dicts=pair[0]
            message=pair[1]
            schema=pair[2]
            table_id=table_fn(message)
            tables = self.bigquery_client.list_tables(options.view_as(GoogleCloudOptions).project + "." + dataset)
            list_id=[]
            for table in tables:
                list_id.append(table.table_id)
            if message["destination"] not in list_id:
                table = bigquery.Table(table_id, schema=schema)#getSchema(message)["fields"])
                table = self.bigquery_client.create_table(table)

            self.bigquery_client.insert_rows_json(table_id, dicts)
    
    window_duration = 60
    ##allowed_lateness = opts.allowed_lateness
    ##dead_letter_bucket = opts.dead_letter_bucket



    p = beam.Pipeline(options=options)


    readData=(p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(input_topic)
            | 'ParseJson' >> beam.Map(parse_json)
            #| 'With timestamps' >> beam.Map(
            #    lambda message: beam.window.TimestampedValue(message, time.time_ns()))
            )

    #partData=(readData | beam.Partition(partition_fn, 2))


    
    data_view=(readData
            | 'Read CSV files' >> beam.ParDo(ReadCsv()))

    data_view | 'WriteWithDynamicDestination' >> beam.ParDo(InsertCsv())


    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
  run()
    

    
