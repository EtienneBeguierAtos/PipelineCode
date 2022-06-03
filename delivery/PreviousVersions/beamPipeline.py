import argparse
import time
import logging
import json
import csv
import typing
from datetime import datetime
from constant import tables, table_schema2, table_schema
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AfterProcessingTime
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.runners import DataflowRunner, DirectRunner
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
input_topics = ['projects/smartlive/topics/my_topic1',
                    'projects/smartlive/topics/my_topic2',
                    'projects/smartlive/topics/my_topic3']

OPTIONS = PipelineOptions( save_main_session=True, streaming=True)
OPTIONS.view_as(GoogleCloudOptions).project = 'smartlive'
OPTIONS.view_as(GoogleCloudOptions).region = 'europe-west1'
OPTIONS.view_as(GoogleCloudOptions).staging_location = 'gs://testinsertbigquery/staging'
OPTIONS.view_as(GoogleCloudOptions).temp_location = 'gs://testinsertbigquery/temp'
OPTIONS.view_as(GoogleCloudOptions).dataflow_service_options=["enable_prime"]
OPTIONS.view_as(DebugOptions).experiments=["use_runner_v2"]
OPTIONS.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('chargement-fichier-streaming-primerunnerv2',time.time_ns())
OPTIONS.view_as(StandardOptions).runner = 'DataflowRunner'
# ### functions and classes

def getSchema(element):
    names=element["address"].split("/")
    if not names[-1] in tables:
        return tables["tabledata.csv"]
    return tables[names[-1]]


def parse_json(element):
    row = json.loads(element.decode('utf-8'))
    #embed()
    return row #CommonLog(**row)




def parse_csv(element, message):
    lines=element.split(",")
    #head=lines[0]
    #lines.pop(0)
    schema=tables["tabledata.csv"]["fields"]
    names=message["address"].split("/")
    if names[-1] in tables:
        schema= tables[names[-1]]["fields"]

    #schema=getSchema(message)["fields"]
    dict={}
    for col in range(len(schema)):
        dict[schema[col]["name"]]=lines[col]
    #embed()
    return dict


def getURI(element):
    return element["address"]

def getDest(element):
    return element["destination"]

def cross_join(left, rights):
    #embed()
    return [left, rights]


# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into a dynamic BigQuery output')

    

    # Setting up the Beam pipeline options

    input_topic = 'projects/smartlive/topics/my_topic2'

    dataset='EtienneResults'
    
    table_names = ['smartlive:EtienneResults.output1', 'smartlive:EtienneResults.output2', 'smartlive:EtienneResults.output3']


    

    def table_fn(row, element):
        #embed()
        destination=element["destination"]
        return OPTIONS.view_as(GoogleCloudOptions).project + ":" + dataset + "."+ destination
    
    window_duration = 60
    ##allowed_lateness = opts.allowed_lateness
    ##dead_letter_bucket = opts.dead_letter_bucket



    p = beam.Pipeline(options=OPTIONS)


    reads=[]
    for topic in input_topics:
        reads.append(beam.io.gcp.pubsub.PubSubSourceDescriptor(topic))


    readData=(p 
            | 'MultipleReadFromPubSub' >> beam.io.MultipleReadFromPubSub(reads)
            | 'ParseJson' >> beam.Map(parse_json))


    dest_view=beam.pvalue.AsSingleton(
        readData | "Window1 into Fixed Intervals" >> beam.WindowInto
        (beam.window.FixedWindows(1),
         trigger=AfterCount(1),
         accumulation_mode=AccumulationMode.DISCARDING))

    
    data_view=(readData
            | 'Get CSV address' >> beam.Map(getURI)
            | 'Read CSV files' >> beam.io.ReadAllFromText(skip_header_lines=1)
            #| beam.GroupBy(lambda s: 0)
            #| 'ParseCSV' >> beam.Map(parse_csv)
            | "Window2 into Fixed Intervals" >> beam.WindowInto
        (beam.window.FixedWindows(1),
         trigger=AfterCount(1),
         accumulation_mode=AccumulationMode.DISCARDING))

    (data_view
        | 'ParseCSV' >> beam.Map(parse_csv, message=dest_view)
        | 'WriteWithDynamicDestination' >> beam.io.WriteToBigQuery(
                table=lambda row, dest_view : table_fn(row,dest_view),
                schema=lambda row, dest_view : getSchema(dest_view),
                table_side_inputs=(dest_view, ),
                schema_side_inputs=(dest_view, ),
                additional_bq_parameters={'ignoreUnknownValues': True},
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                ))
                 

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
  run()
    

    
