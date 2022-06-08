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
from constants import MESSAGE_SCHEMA,TABLE_OUTPUT_SCHEMAS, STRUCTURE_SOURCE, REJECT_SCHEMA,MONITORING_TABLE_SCHEMA
#from IPython import embed
import sys

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

parameters={"skipLeadingRows":0}

parser = argparse.ArgumentParser()
parser.add_argument('--skipLeadingRows',default=1, type=int)

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

def check_errors_in_line(line,schema):
    if len(line)!=len(schema):
        return "error_line_index"
    else:
        return ""

       


class ReadCsv(beam.DoFn):
    #def setup(self):
    #    from google.cloud import storage
    #    logging.warning('Creation Storage client!')
    #    self.storage_client = storage.Client()
    
    def process(self, message, timestamp=beam.DoFn.TimestampParam):
        with beam.io.gcsio.GcsIO().open(filename=message["fileURL"], mode="r") as f:
            file_name=message["fileURL"].split("/")[-1]
            file_struct=get_struct_file(file_name)
            file_lines=f.read().decode(file_struct['encoding']).split(file_struct["delimiter"])
            file_max_bad_rows=file_struct["max_bad_rows"]
            
            logging.warning('Data processed: '+file_name)

            line_count = 0
            schema=file_struct["Schema"]#dataframeToSchema(out)
            rejected_lines_count=0
            #embed()
            output_rows={"rows":[], "timestamp":timestamp.to_utc_datetime(), "is_valid_file":True}
            for line in file_lines:
                if line_count<parameters["skipLeadingRows"]:
                    line_count+=1
                    continue
                output_row={"stacktrace":"","source_file":file_name}
                splitted_line=line.split(",")
                line_error=check_errors_in_line(splitted_line, schema)
                if line_error=="":
                    for index in range(len(schema)):
                        output_row[schema[index]['name']]=splitted_line[index]
                else:
                    rejected_lines_count+=1
                output_row["stacktrace"]=line_error
                output_row["destination"]=message["destination"]
                output_row["timestamp"]=timestamp.to_utc_datetime()
                output_rows["rows"].append(output_row)
            if rejected_lines_count>file_max_bad_rows:
                output_rows["is_valid_file"]=False
            output_rows["number_invalid_rows"]=rejected_lines_count
            #embed()
            yield output_rows


class SplitFileData(beam.DoFn):
    #def setup(self):
    #    from google.cloud import storage
    #    logging.warning('Creation Storage client!')
    #    self.storage_client = storage.Client()
    
    def process(self, element):
        for row in element["rows"]:
            if row["stacktrace"]=="" and not element["is_valid_file"]:
                row["stacktrace"]="too_many_invalid_rows_in_file"
            yield row
        


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

def GetMonitoringData(file_data):
    output={"timestamp":file_data["timestamp"]}
    if file_data["is_valid_file"]:
        output["number_inserted_rows"]=len(file_data["rows"])-file_data["number_invalid_rows"]
        output["number_rejected_rows"]=file_data["number_invalid_rows"]
    else:
        output["number_inserted_rows"]=0
        output["number_rejected_rows"]=len(file_data["rows"])
    return output





# ### main

def run():
    # Command line arguments
    opts, pipeline_opts = parser.parse_known_args()


    
    parameters["skipLeadingRows"] = int(opts.skipLeadingRows)

    

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

    monitoring_branch=(data_view
            | 'Format Monitoring Report' >> beam.Map(GetMonitoringData)
            | 'WriteInMonitoringTable'>> beam.io.WriteToBigQuery(
                table=OPTIONS.view_as(GoogleCloudOptions).project + ":" + DATASET + ".monitoring",
                schema=MONITORING_TABLE_SCHEMA,
                ignore_unknown_columns=True,
                #additional_bq_parameters={'ignoreUnknownValues': True,'maxBadRecords': 1000},
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                ))
    
    rows_view=(data_view | 'Split file data in rows' >> beam.ParDo(SplitFileData()))

    #groups=(({"messages":readData,"csv":data_view})
    #    | 'Merge' >> beam.CoGroupByKey()
    #    | beam.Map(getResults))

    (rows_view 
            | 'Filter rejects' >> beam.Filter(is_rejected)
            | 'Format error reports' >> beam.ParDo(GetRejectData())
            | 'WriteInRejectsTable'>> beam.io.WriteToBigQuery(
                table=OPTIONS.view_as(GoogleCloudOptions).project + ":" + DATASET + ".rejects",
                schema=REJECT_SCHEMA,
                ignore_unknown_columns=True,
                #additional_bq_parameters={'ignoreUnknownValues': True,'maxBadRecords': 1000},
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                ))

    (rows_view 
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

    p.run()#.wait_until_finish()

if __name__ == '__main__':
    run()