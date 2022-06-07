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
from constants import MESSAGE_SCHEMA,TABLE_OUTPUT_SCHEMAS, STRUCTURE_SOURCE, REJECT_SCHEMA
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


INPUT_TOPICS = ['projects/smartlive/topics/my_topic1',
                    'projects/smartlive/topics/my_topic2',
                    'projects/smartlive/topics/my_topic3']

DATASET='EtienneResults'

skipLeadingRows=0

# ### functions and classes


def get_struct_file(element):
    keys=STRUCTURE_SOURCE.keys()
    for key in keys:
        if re.match(key, element):
            return STRUCTURE_SOURCE[key]
    logging.warning("no structure associated to filename: "+element)
    return "unavailable"#alert




def parse_json(element):
    #from schema import Schema
    #MessageSchema = Schema({"fileURL": str,"destination":str})
    row = json.loads(element.decode('utf-8'))
    isValid=MESSAGE_SCHEMA.validate(row)
    return row #CommonLog(**row)

def is_rejected(element):
    return element["stacktrace"]!=""

def is_valid(element):
    return element["stacktrace"]==""


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
            #embed()
            for line in file_lines:
                output_row={"stacktrace":"","source_file":file_name}
                splitted_line=line.split(",")
                if line_count<skipLeadingRows:
                    line_count+=1
                elif len(splitted_line)<len(schema):
                    output_row["stacktrace"]="error_line_index"
                    yield output_row
                else:
                    for index in range(len(schema)):
                        output_row[schema[index]['name']]=splitted_line[index]
                    output_row["destination"]=message["destination"]
                    yield output_row
        


def get_destination_table(element):
    destination=element["destination"]
    return OPTIONS.view_as(GoogleCloudOptions).project + ":" + DATASET + "."+ destination


def get_schema(table):
    address=table.split(".")
    table_name=address[-1]
    schema={"fields":TABLE_OUTPUT_SCHEMAS[table_name]}
    return schema


class GetRejectData(beam.DoFn):
  def process(self, element, timestamp=beam.DoFn.TimestampParam):
    yield {"timestamp":timestamp.to_utc_datetime(), "stacktrace":element["stacktrace"], "source_file":element["source_file"]}



#def getResults(element):
#    embed()
#    return element


NUMBER_INSERTED_LINES=0
NUMBER_REJECTED_LINES=0




# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--skipLeadingRows')
    opts, pipeline_opts = parser.parse_known_args()


    global skipLeadingRows
    skipLeadingRows = int(opts.skipLeadingRows)


    

    ##allowed_lateness = opts.allowed_lateness
    ##dead_letter_bucket = opts.dead_letter_bucket



    p = beam.Pipeline(options=OPTIONS)

    reads=[]
    for topic in INPUT_TOPICS:
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

    (data_view 
            | 'Filter rejects' >> beam.Filter(is_rejected)
            | 'Format error reports' >> beam.ParDo(GetRejectData())
            | 'writeInRejectsTable'>> beam.io.WriteToBigQuery(
                table=OPTIONS.view_as(GoogleCloudOptions).project + ":" + DATASET + ".rejects",
                schema=REJECT_SCHEMA,
                ignore_unknown_columns=True,
                #additional_bq_parameters={'ignoreUnknownValues': True,'maxBadRecords': 1000},
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                ))

    (data_view 
            | 'Get valid lines' >> beam.Filter(is_valid)
            | 'WriteWithDynamicDestination' >> beam.io.WriteToBigQuery(
                table=get_destination_table,
                schema=get_schema,
                ignore_unknown_columns=True,
                #additional_bq_parameters={'ignoreUnknownValues': True,'maxBadRecords': 1000},
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                ))

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
  run()