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
from schema import Schema, And, Use, Optional, SchemaError
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AfterProcessingTime, AccumulationMode
import pandas as pd
import io
import re
import logging
#from IPython import embed

# Setting up the Beam pipeline options
OPTIONS = PipelineOptions( save_main_session=True, streaming=True)
OPTIONS.view_as(GoogleCloudOptions).project = 'smartlive'
OPTIONS.view_as(GoogleCloudOptions).region = 'europe-west1'
OPTIONS.view_as(GoogleCloudOptions).staging_location = 'gs://testinsertbigquery/staging'
OPTIONS.view_as(GoogleCloudOptions).temp_location = 'gs://testinsertbigquery/temp'
OPTIONS.view_as(GoogleCloudOptions).dataflow_service_options=["enable_prime"]
OPTIONS.view_as(DebugOptions).experiments=["use_runner_v2"]
OPTIONS.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('chargement-fichier-streaming',time.time_ns())
OPTIONS.view_as(SetupOptions).requirements_file="requirements.txt"
OPTIONS.view_as(StandardOptions).runner = 'DataflowRunner'


input_topics = ['projects/smartlive/topics/my_topic1',
                    'projects/smartlive/topics/my_topic2',
                    'projects/smartlive/topics/my_topic3']

dataset='EtienneResults'



# ### functions and classes

MESSAGE_SCHEMA = Schema({"fileURL": str,
"destination":str})

STRUCTURE_SOURCE={
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


TABLE_OUTPUT_SCHEMAS={
    "etienne12":[
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
    ],
    "etienne13":[
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
    ],
    "etienne14":[
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


def get_struct_file(element):
    keys=STRUCTURE_SOURCE.keys()
    for key in keys:
        if re.match(key, element):
            return STRUCTURE_SOURCE[key]
    return "unavailable"#alert




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
    #def setup(self):
    #    from google.cloud import storage
    #    logging.warning('Creation Storage client!')
    #    self.storage_client = storage.Client()
    
    def process(self, message):
        with beam.io.gcsio.GcsIO().open(filename=message["fileURL"], mode="r") as f:
            file_name=message["fileURL"].split("/")[-1]
            file_struct=get_struct_file(file_name)
            file_lines=f.read().decode(file_struct['encoding']).split(file_struct["delimiter"])
            
            logging.warning('Data processed: '+file_name)

            line_count = 0
            schema=file_struct["Schema"]#dataframeToSchema(out)
            for line in file_lines:
                output_row={}
                splitted_line=line.split(",")
                if line_count==0:
                    line_count+=1
                else:
                    for index in range(len(schema)):
                        output_row[schema[index]['name']]=splitted_line[index]
                    output_row["destination"]=message["destination"]
                    #embed()
                    yield output_row
        


def get_destination_table(element):
    destination=element["destination"]
    return OPTIONS.view_as(GoogleCloudOptions).project + ":" + dataset + "."+ destination


def get_schema(table):
    address=table.split(".")
    table_name=address[-1]
    schema={"fields":TABLE_OUTPUT_SCHEMAS[table_name]}
    return schema



#def getResults(element):
#    embed()
#    return element







# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into a dynamic BigQuery output')

    

    ##allowed_lateness = opts.allowed_lateness
    ##dead_letter_bucket = opts.dead_letter_bucket



    p = beam.Pipeline(options=OPTIONS)

    reads=[]
    for topic in input_topics:
        reads.append(beam.io.gcp.pubsub.PubSubSourceDescriptor(topic))



    readData=(p
            | 'MultipleReadFromPubSub' >> beam.io.MultipleReadFromPubSub(reads)
            | 'ParseJson' >> beam.Map(parse_json)
            )

    #partData=(readData | beam.Partition(partition_fn, 2))


    
    data_view=(readData
            #| 'Get CSV address' >> beam.Map(lambda element: element["fileURL"])
            | 'Read CSV files' >> beam.ParDo(ReadCsv()))#beam.io.textio.ReadFromTextWithFilename("gs://testinsertbigquery/EtienneData/client1.csv"))

    
    #groups=(({"messages":readData,"csv":data_view})
    #    | 'Merge' >> beam.CoGroupByKey()
    #    | beam.Map(getResults))

    data_view | 'WriteWithDynamicDestination' >> beam.io.WriteToBigQuery(
                table=get_destination_table,
                schema=get_schema,
                ignore_unknown_columns=True,
                #additional_bq_parameters={'ignoreUnknownValues': True,'maxBadRecords': 1000},
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
  run()
    

    
