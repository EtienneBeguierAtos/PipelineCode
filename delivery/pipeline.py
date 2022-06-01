import argparse
from asyncio import constants
import io
import time
import logging
import json
import typing
from datetime import datetime
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners import DataflowRunner, DirectRunner
from google.cloud import storage
from google.cloud import bigquery
from schema import Schema, And, Use, Optional, SchemaError
import pandas as pd
import io
import re
#from IPython import embed

# Setting up the Beam pipeline options
Options = PipelineOptions( save_main_session=True, streaming=True)
Options.view_as(GoogleCloudOptions).project = 'smartlive'
Options.view_as(GoogleCloudOptions).region = 'europe-west1'
Options.view_as(GoogleCloudOptions).staging_location = 'gs://testinsertbigquery/staging'
Options.view_as(GoogleCloudOptions).temp_location = 'gs://testinsertbigquery/temp'
Options.view_as(GoogleCloudOptions).dataflow_service_options=["enable_prime"]
Options.view_as(DebugOptions).experiments=["use_runner_v2"]
Options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('chargement-fichier-streaming-primerunnerv2',time.time_ns())
Options.view_as(SetupOptions).requirements_file="requirements.txt"
Options.view_as(StandardOptions).runner = 'DataflowRunner'


input_topic = 'projects/smartlive/topics/my_topic3'

input_topics = ['projects/smartlive/topics/my_topic1',
                    'projects/smartlive/topics/my_topic2',
                    'projects/smartlive/topics/my_topic3']

dataset='EtienneResults'



# ### functions and classes

MessageSchema = Schema({"fileURL": str,
"destination":str})

Constants={
    "client":{ 
        "delimiter": ";",
        "max_bad_rows" : 18,
        "encoding": "utf-8",
        "Schema" :[
        {
            "name": "NAME",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "AGE",
            "mode": "NULLABLE",
            "type": "INTEGER"
        }
    ]  
  },
  "telephone": { 
        "delimiter": ";",
        "max_bad_rows" : 18,
        "encoding": "utf-8",
        "Schema" :[
        {
            "name": "name",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "number",
            "mode": "NULLABLE",
            "type": "INTEGER"
        }
    ]  
  },
  "address": { 
        "delimiter": ";",
        "max_bad_rows" : 18,
        "encoding": "utf-8",
        "Schema" :[
        {
            "name": "name",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "address",
            "mode": "NULLABLE",
            "type": "STRING"
        }
    ]  
  }
}


def getRegexCase(element):
    keys=Constants.keys()
    for key in keys:
        if re.match(key, element):
            return Constants[key]
    return "unavailable"




def parse_json(element):
    from schema import Schema
    MessageSchema = Schema({"fileURL": str,"destination":str})
    row = json.loads(element.decode('utf-8'))
    isValid=MessageSchema.validate(row)
    return row #CommonLog(**row)



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
        from google.cloud import storage
        self.storage_client = storage.Client()
    
    def process(self, message):

        address=message["fileURL"].split("/")
        bucket = self.storage_client.get_bucket(address[2])
        blob_address=address[3]
        paramsDict=getRegexCase(address[-1])
        for i in range(4, len(address)):
            blob_address+="/"+address[i]
        blob = bucket.get_blob(blob_address)
        downloaded_blob = blob.download_as_string().decode(paramsDict["encoding"])
        buffer=io.StringIO(downloaded_blob)
        out = pd.read_csv(filepath_or_buffer = buffer)
        lines=downloaded_blob.split(paramsDict["delimiter"])

        line_count = 0
        schema=paramsDict["Schema"]#dataframeToSchema(out)
        schemal=[]
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
        #embed()
        yield [list_dict,message,schema]


def get_destination_table(element):
        destination=element["destination"]
        return Options.view_as(GoogleCloudOptions).project + "." + dataset + "."+ destination


class InsertCsv(beam.DoFn):
    def start_bundle(self):
        from google.cloud import bigquery
        self.bigquery_client = bigquery.Client()

    def process(self, pair):
        from google.cloud import bigquery

        dicts=pair[0]
        message=pair[1]
        schema=pair[2]
        table_id=get_destination_table(message)
        tables = self.bigquery_client.list_tables(Options.view_as(GoogleCloudOptions).project + "." + dataset)
        list_id=[]
        for table in tables:
            list_id.append(table.table_id)
        if message["destination"] not in list_id:
            table = bigquery.Table(table_id, schema=schema)
            table = self.bigquery_client.create_table(table)
        else:
            comp_schema=Schema(schema)
            dest_schema=self.bigquery_client.get_table(table_id).schema
            parse_schema=[]
            for field in dest_schema:
                parse_schema.append({
                    'name':field.name,
                    "mode": field.mode,
                    'type':field.field_type
                    }
                )
            comp_schema.validate(parse_schema)
        self.bigquery_client.insert_rows_json(table_id, dicts)





# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into a dynamic BigQuery output')

    

    ##allowed_lateness = opts.allowed_lateness
    ##dead_letter_bucket = opts.dead_letter_bucket



    p = beam.Pipeline(options=Options)

    reads=[]
    for topic in input_topics:
        reads.append(beam.io.gcp.pubsub.PubSubSourceDescriptor(topic))



    readData=(p
            | 'MultipleReadFromPubSub' >> beam.io.MultipleReadFromPubSub(reads)
            | 'ParseJson' >> beam.Map(parse_json)
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
    

    
