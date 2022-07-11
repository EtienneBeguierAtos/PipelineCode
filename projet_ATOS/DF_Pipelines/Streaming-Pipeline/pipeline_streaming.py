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
import os
import copy
#from constants import MESSAGE_SCHEMA,TABLE_OUTPUT_SCHEMAS, STRUCTURE_SOURCE, REJECT_SCHEMA,MONITORING_TABLE_SCHEMA
from custom_read_streaming import custom_read_file
from pipeline_commons import pipeline_commons as commons
#from IPython import embed
#import sys

# Setting up the Beam pipeline options


INPUT_TOPICS = ['projects/smartlive/topics/my_topic1',
                    'projects/smartlive/topics/my_topic2',
                    'projects/smartlive/topics/my_topic3']

DATASET='EtienneResults'
REJECT_DATASET='EtienneRejects'

parameters={"skipLeadingRows":1}

PARSER = argparse.ArgumentParser()
PARSER.add_argument('--skipLeadingRows',default=1, type=int)
PARSER.add_argument('--runner',default='DataflowRunner', type=str)

# ### functions and classes

class get_timestamp(beam.DoFn):
    
    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        #embed()
        yield {"timestamp":timestamp.to_utc_datetime(),"message":element}
        


def parse_json(element):
    #from schema import Schema
    #MessageSchema = Schema({"fileURL": str,"destination":str})["data"]
    row = json.loads(element["message"].data.decode('utf-8'))
    #try:
    #    isValid=commons.MESSAGE_SCHEMA.validate(row)
    #except SchemaError:
    #    return {"stacktrace": "invalid_message_format","timestamp":element["timestamp"]}
    #else:
    row["timestamp"]=element["timestamp"]
    row["id"]=element["message"].message_id
    #logging.warning('row:  '+str(row))
    return row

def read_line(element):
    #embed()
    line=element["data"]
    message=element["message"]
    #embed()
    file_name=message["fileURL"].split("/")[-1]
    file_struct=commons.get_struct_file(file_name)
    schema=file_struct["Schema"]
    output_row={"stacktrace":"","source_file":file_name}
    splitted_line=line.split(",")
    line_error=commons.check_errors_in_line(splitted_line, schema)
    if line_error=="":
        for index in range(len(schema)):
            output_row[schema[index]['name']]=commons.dictTypes[schema[index]['type']](splitted_line[index])
    output_row["raw_data"]=splitted_line
    output_row["stacktrace"]=line_error
    output_row["destination"]=message["destination"]
    output_row["timestamp"]=message["timestamp"]
    output_row["id"]=message["id"]
    if "partition_depth" in message.keys() and "partition_key" in message.keys():
        output_row["partition_depth"]=message["partition_depth"]
        output_row["partition_key"]=message["partition_key"]
    #logging.info(str(output_row))
    return (str(message["id"])+" "+str(message["timestamp"]),output_row)

class SplitFileData(beam.DoFn):
    
    def process(self, element):
        for row in element["rows"]:
            if row["stacktrace"]=="" and not element["is_valid_file"]:
                row["stacktrace"]="too_many_invalid_rows_in_file"
            yield row

def get_destination_table(element):
    destination=element["destination"]
    return commons.OPTIONS.view_as(GoogleCloudOptions).project + ":" + DATASET + "."+ destination

def get_reject_table(element):
    #embed()
    return commons.OPTIONS.view_as(GoogleCloudOptions).project + ":" + REJECT_DATASET + "."+ element["destination"]

def get_schema(table):
    address=table.split(".")
    table_name=address[-1]
    schema={"fields":commons.TABLE_OUTPUT_SCHEMAS[table_name]}
    return schema

def format_line(element):
    return element[1]

def get_reject_data(element):
    file_data=element[1]
    return {"timestamp":file_data["timestamp"], 
            "stacktrace":file_data["stacktrace"], 
            "source_file":file_data["source_file"],
            "raw_data":str(file_data["raw_data"]),
            "destination":file_data["destination"]}

def get_monitoring_data(groups_of_lines):
    number_inserted_rows=0
    number_rejected_rows=0
    id_and_timestamp=groups_of_lines[0].split(" ")
    valids_and_rejects=groups_of_lines[1]
    if len(valids_and_rejects["valid_count"])>0:
        number_inserted_rows=valids_and_rejects["valid_count"][0]
    if len(valids_and_rejects["rejects_count"])>0:
        number_rejected_rows=valids_and_rejects["rejects_count"][0]
    monitoring_report={ "event":"SUCCESS",
                        "timestamp":id_and_timestamp[1],
                        "number_inserted_rows":number_inserted_rows,
                        "number_rejected_rows": number_rejected_rows,
                        "message_id":id_and_timestamp[0]}
    if number_rejected_rows!=0:
        monitoring_report["event"]="WARNING"
    #embed()
    return monitoring_report



def get_start_monitoring_data(message):
    output={"timestamp":message["timestamp"], 
            "event":"START",
            "number_inserted_rows":0,
            "number_rejected_rows":0}
    return output


def log_element(element):
    logging.warning(str(element))
    return element


# ### main

def run_streaming(runner):

    options=copy.deepcopy(commons.OPTIONS)
    options.view_as(StandardOptions).streaming=True
    options.view_as(StandardOptions).runner=runner

    p = beam.Pipeline(options=options)
    #embed()
    
    reads=[]
    for topic in INPUT_TOPICS:
        reads.append(beam.io.gcp.pubsub.PubSubSourceDescriptor(topic))
    
    read_messages=(p
            | 'MultipleReadFromPubSub' >> beam.io.ReadFromPubSub('projects/smartlive/topics/my_topic1', with_attributes=True)  #beam.io.MultipleReadFromPubSub(reads, with_attributes=True)
            | 'LogElement' >> beam.Map(log_element)
            | 'GetTimestamp' >> beam.ParDo(get_timestamp())
            | 'ParseJson' >> beam.Map(parse_json)
            | "Window into Fixed Intervals" >> beam.WindowInto(
                beam.window.FixedWindows(1),
                #trigger=AfterProcessingTime(5),
                accumulation_mode=AccumulationMode.DISCARDING)
            )
    
    
    start_monitoring=(read_messages
            | 'Format Start Monitoring Report' >> beam.Map(get_start_monitoring_data)
            | 'WriteStartInMonitoringTable'>> beam.io.WriteToBigQuery(
                table=commons.OPTIONS.view_as(GoogleCloudOptions).project + ":" + DATASET + ".monitoring",
                schema=commons.MONITORING_TABLE_SCHEMA,
                ignore_unknown_columns=True,
                #additional_bq_parameters={'ignoreUnknownValues': True,'maxBadRecords': 1000},
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                ))
    
    
    
    data=(read_messages
            | 'Read CSV files' >> custom_read_file.custom_ReadAllFromText(skip_header_lines=parameters["skipLeadingRows"])
            | 'Read lines' >> beam.Map(read_line)#beam.io.textio.ReadFromTextWithFilename("gs://testinsertbigquery/EtienneData/client1.csv"))
            | 'Filter Partition' >> beam.Map(commons.check_partition))

    
    rejected_data=data | 'Filter rejects' >> beam.Filter(lambda element: element[1]["stacktrace"]!="")

    rejects_count=rejected_data | 'Count rejected lines' >> beam.combiners.Count.PerKey()

    (rejected_data 
        | 'Format error reports' >> beam.Map(get_reject_data)
        | 'WriteInRejectsTable'>> beam.io.WriteToBigQuery(
            table=get_reject_table,
            schema=commons.REJECT_SCHEMA,
            ignore_unknown_columns=True,
            #additional_bq_parameters={'ignoreUnknownValues': True,'maxBadRecords': 1000},
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            ))
    
    

    valid_data=data | 'Get valid lines' >> beam.Filter(lambda element: element[1]["stacktrace"]=="")

    valid_count=valid_data | 'Count valid lines' >> beam.combiners.Count.PerKey()

    (valid_data 
        | 'Format lines' >> beam.Map(format_line)
        | 'WriteWithDynamicDestination' >> beam.io.WriteToBigQuery(
                table=get_destination_table,
                schema=get_schema,
                ignore_unknown_columns=True,
                #additional_bq_parameters={'ignoreUnknownValues': True,'maxBadRecords': 1000},
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                ))

    monitoring_branch=({"valid_count":valid_count, "rejects_count":rejects_count}
        | beam.CoGroupByKey()
        | 'Format Monitoring Report' >> beam.Map(get_monitoring_data)
        | 'WriteInMonitoringTable'>> beam.io.WriteToBigQuery(
                table=commons.OPTIONS.view_as(GoogleCloudOptions).project + ":" + DATASET + ".monitoring",
                schema=commons.MONITORING_TABLE_SCHEMA,
                ignore_unknown_columns=True,
                #additional_bq_parameters={'ignoreUnknownValues': True,'maxBadRecords': 1000},
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                ))
    
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()







if __name__ == '__main__':
    opts, pipeline_opts = PARSER.parse_known_args()
    parameters["skipLeadingRows"] = int(opts.skipLeadingRows)
    runner=str(opts.runner)
    run_streaming(runner)