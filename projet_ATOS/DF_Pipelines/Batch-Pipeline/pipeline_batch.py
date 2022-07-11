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
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AfterProcessingTime, AccumulationMode
from apache_beam.options.value_provider import StaticValueProvider
from pipeline_commons import pipeline_commons as commons
import io
import re
import logging
import os
#from constants import MESSAGE_SCHEMA,TABLE_OUTPUT_SCHEMAS, STRUCTURE_SOURCE, REJECT_SCHEMA,MONITORING_TABLE_SCHEMA
#from custom_read_file import custom_read_file
import copy
#from IPython import embed
#import sys

# Setting up the Beam pipeline options


parameters={"skipLeadingRows":1}

PARSER = argparse.ArgumentParser()
PARSER.add_argument('--skipLeadingRows',default=1, type=int)

# ### functions and classes




class get_timestamp(beam.DoFn):
    #def setup(self):
    #    from google.cloud import storage
    #    logging.warning('Creation Storage client!')
    #    self.storage_client = storage.Client()
    
    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        #embed()
        yield {"timestamp":timestamp.to_utc_datetime(),"message":element}
        


def check_errors_in_line(line,schema):
    if len(line)!=len(schema):
        return "error_line_index"
    else:
        return ""



    #def setup(self):
    #    from google.cloud import storage
    #    logging.warning('Creation Storage client!')
    #    self.storage_client = storage.Client()

def read_line_batch(line,fileURL,timestamp,id):
    #embed()
    file_struct=commons.get_struct_file(fileURL.get().split("/")[-1])
    schema=file_struct["Schema"]
    output_row={"stacktrace":""}
    splitted_line=line.split(",")
    line_error=check_errors_in_line(splitted_line, schema)
    if line_error=="":
        for index in range(len(schema)):
            output_row[schema[index]['name']]=commons.dictTypes[schema[index]['type']](splitted_line[index])
    else:
        output_row["source_file"]=fileURL.get().split("/")[-1]
        output_row["raw_data"]=splitted_line
        output_row["timestamp"]=timestamp.get()
        output_row["id"]=id.get()
    output_row["stacktrace"]=line_error
    #embed()
    return (str(id.get())+" "+str(timestamp.get()),output_row)



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


def get_reject_data(element):
    file_data=element[1]
    return {"timestamp":file_data["timestamp"], 
            "stacktrace":file_data["stacktrace"], 
            "source_file":file_data["source_file"],
            "raw_data":str(file_data["raw_data"])}

def get_monitoring_data_batch(groups_of_lines, timestamp):
    number_inserted_rows=0
    number_rejected_rows=0
    valids_and_rejects=groups_of_lines[1]
    if len(valids_and_rejects["valid_count"])>0:
        number_inserted_rows=valids_and_rejects["valid_count"][0]
    if len(valids_and_rejects["rejects_count"])>0:
        number_rejected_rows=valids_and_rejects["rejects_count"][0]
    monitoring_report={ "event":"SUCCESS",
                        "timestamp":timestamp.get(),
                        "number_inserted_rows":number_inserted_rows,
                        "number_rejected_rows": number_rejected_rows}
                       # "message_id":message["id"]}
    if number_rejected_rows!=0:
        monitoring_report["event"]="WARNING"
    #embed()
    return monitoring_report


def get_start_monitoring_data(message):
    #file_name=message["fileURL"].split("/")[-1]
    output={"timestamp":message["timestamp"].get(), 
            "event":"START",
            "number_inserted_rows":0,
            "number_rejected_rows":0}
    return output


def log_element(element):
    #embed()
    return element

def format_line(element):
    line=copy.deepcopy(element[1])
    del line["stacktrace"]
    logging.warning(str(line))
    return line

def create_json(element,timestamp):
    return {"timestamp": timestamp.get()}

class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--fileURL',default="gs://etienne_files/telephone2.csv", type=str)
        parser.add_value_provider_argument('--destination',default='telephone', type=str)
        parser.add_value_provider_argument('--timestamp',default='1111', type=str)
        parser.add_value_provider_argument('--id',default='00000', type=str)



# ### main

def run_batch(runner):
    options=commons.OPTIONS
    options.view_as(StandardOptions).streaming=False
    options.view_as(StandardOptions).runner=runner


    p = beam.Pipeline(options=options)

    user_options = options.view_as(UserOptions)

    #message=json.loads(user_options.message)

    
    read_messages=(p | 'CreateMessage' >> beam.Create([{}])
    | beam.Map(create_json,user_options.timestamp))

    (read_messages | commons.start_monitoring())
    
    data=(p
                | 'Read CSV files' >> beam.io.ReadFromText(user_options.fileURL,skip_header_lines=parameters["skipLeadingRows"]) #beam.io.ReadAllFromText(skip_header_lines=parameters["skipLeadingRows"])
                #| 'Split file lines' >> beam.Map(split_file)
                #| 'Get individual lines' >> beam.FlatMap(separate_lines)
                | 'ReadLines' >> beam.Map(
                    read_line_batch, 
                    fileURL=user_options.fileURL,
                    timestamp=user_options.timestamp,
                    id=user_options.id)
                | 'Filter Partition' >> beam.Map(commons.check_partition))

    rejected_data=data | 'Filter rejects' >> beam.Filter(lambda element: element[1]["stacktrace"]!="")

    rejects_count=rejected_data | 'Count rejected lines' >> beam.combiners.Count.PerKey()

    (rejected_data
            | 'Format error reports' >> beam.Map(get_reject_data)
            | 'WriteInRejectsTable'>> beam.io.WriteToBigQuery(
                table=lambda element :commons.OPTIONS.view_as(GoogleCloudOptions).project + ":" + commons.REJECT_DATASET + "."+ user_options.destination.get(),
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
                table=lambda element :commons.OPTIONS.view_as(GoogleCloudOptions).project + ":" + commons.DATASET + "."+ user_options.destination.get(), #get_destination_table,
                schema=commons.get_schema,
                ignore_unknown_columns=True,
                #additional_bq_parameters={'ignoreUnknownValues': True,'maxBadRecords': 1000},
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                ))
    
    monitoring_branch=({"valid_count":valid_count, "rejects_count":rejects_count}
            | beam.CoGroupByKey()
            | 'Format Monitoring Report' >> beam.Map(get_monitoring_data_batch, user_options.timestamp)
            | 'WriteInMonitoringTable'>> beam.io.WriteToBigQuery(
                table=commons.OPTIONS.view_as(GoogleCloudOptions).project + ":" + commons.DATASET + ".monitoring",
                schema=commons.MONITORING_TABLE_SCHEMA,
                ignore_unknown_columns=True,
                #additional_bq_parameters={'ignoreUnknownValues': True,'maxBadRecords': 1000},
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                ))
    
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()#.wait_until_finish()


if __name__ == '__main__':
    opts, pipeline_opts = PARSER.parse_known_args()
    parameters["skipLeadingRows"] = int(opts.skipLeadingRows)
    run_batch("DataflowRunner")
    #{
    #            "fileURL":"gs://etienne_files/telephone4.csv",
    #            "destination":"telephone",
    #            "timestamp" : datetime.utcnow().strftime("%Y%m%d"),
    #            "id": "000000"
    #        })