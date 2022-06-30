import argparse
import time
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners import DataflowRunner, DirectRunner
from schema import Schema
#from constants import STRUCTURE_SOURCE, TABLE_OUTPUT_SCHEMAS
import re
import logging
#from IPython import embed
#import sys


OPTIONS = PipelineOptions( save_main_session=True, streaming=True)
OPTIONS.view_as(GoogleCloudOptions).project = 'smartlive'
OPTIONS.view_as(GoogleCloudOptions).region = 'europe-west1'
OPTIONS.view_as(GoogleCloudOptions).staging_location = 'gs://etienne_files/staging'
OPTIONS.view_as(GoogleCloudOptions).temp_location = 'gs://etienne_files/temp'
OPTIONS.view_as(GoogleCloudOptions).dataflow_service_options=["enable_prime"]
OPTIONS.view_as(DebugOptions).experiments=["use_runner_v2","use_deprecated_read"]
OPTIONS.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('chargement-fichier-streaming',time.time_ns())
OPTIONS.view_as(SetupOptions).requirements_file="requirements.txt"
OPTIONS.view_as(StandardOptions).runner = 'DataflowRunner'
OPTIONS.view_as(SetupOptions).setup_file='./setup.py'

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
            "type": "STRING"
        }
    ]  
  },
  "telephone": { 
        "delimiter": "\r\n", #";",
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
            "type": "STRING"
        }
    ]  
  },
  "address": { 
        "delimiter": ";",
        "max_bad_rows" : 0,
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
    "age":[
        {
            "name": "NAME",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "AGE",
            "mode": "NULLABLE",
            "type": "STRING"
        }
    ],
    "telephone":[
        {
            "name": "name",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "number",
            "mode": "NULLABLE",
            "type": "STRING"
        }
    ],
    "address":[
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


REJECT_SCHEMA={
    "fields":[
        {
            "name": "timestamp",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "stacktrace",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "source_file",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "raw_data",
            "mode": "NULLABLE",
            "type": "STRING"
        }]}


MONITORING_TABLE_SCHEMA={
    "fields":[
        {
            "name": "timestamp",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "event",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "number_inserted_rows",
            "mode": "NULLABLE",
            "type": "INTEGER"
        },
        {
            "name": "number_rejected_rows",
            "mode": "NULLABLE",
            "type": "INTEGER"
        }]}

dictTypes={
    'int64':'INTEGER',
    'object':'STRING',
    'float64':'FLOAT'
}

DATASET='EtienneResults'
REJECT_DATASET='EtienneRejects'

def get_struct_file(element):
    keys=STRUCTURE_SOURCE.keys()
    for key in keys:
        if re.match(key, element):
            return STRUCTURE_SOURCE[key]
    logging.warning("no structure associated to filename: "+element)
    return "unavailable"#alert

def get_schema(table):
    address=table.split(".")
    table_name=address[-1]
    schema={"fields":TABLE_OUTPUT_SCHEMAS[table_name]}
    return schema

def check_errors_in_line(line,schema):
    if len(line)!=len(schema):
        return "error_line_index"
    else:
        return ""

def get_start_monitoring_data(message):
    #file_name=message["fileURL"].split("/")[-1]
    output={"timestamp":message["timestamp"], 
            "event":"START",
            "number_inserted_rows":0,
            "number_rejected_rows":0}
    return output

class start_monitoring(beam.PTransform):
    def expand(self,pcoll):
        return (pcoll
            | 'Format Start Monitoring Report' >> beam.Map(get_start_monitoring_data)
            | 'WriteStartInMonitoringTable'>> beam.io.WriteToBigQuery(
                table=OPTIONS.view_as(GoogleCloudOptions).project + ":" + DATASET + ".monitoring",
                schema=MONITORING_TABLE_SCHEMA,
                ignore_unknown_columns=True,
                #additional_bq_parameters={'ignoreUnknownValues': True,'maxBadRecords': 1000},
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))