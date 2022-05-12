import argparse
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
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AfterProcessingTime
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.runners import DataflowRunner, DirectRunner
from IPython import embed

# ### functions and classes



def parse_json(element):
    row = json.loads(element.decode('utf-8'))
    row.pop("target_table", None)
    #embed()
    return row #CommonLog(**row)

def parse_json2(element):
    row = json.loads(element.decode('utf-8'))
    #embed()
    return row["target_table"] #CommonLog(**row)




# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into BigQuery')

    

    # Setting up the Beam pipeline options
    options = PipelineOptions( save_main_session=True, streaming=True)
    options.view_as(GoogleCloudOptions).project = 'smartlive'
    options.view_as(GoogleCloudOptions).region = 'europe-west1'
    options.view_as(GoogleCloudOptions).staging_location = 'gs://testinsertbigquery/staging'
    options.view_as(GoogleCloudOptions).temp_location = 'gs://testinsertbigquery/temp'
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('dynamic-output-streaming-',time.time_ns())
    options.view_as(StandardOptions).runner = 'DirectRunner'

    input_topic = 'projects/smartlive/topics/my_topic1'
    
    table_names = ['smartlive:EtienneResults.output1', 'smartlive:EtienneResults.output2', 'smartlive:EtienneResults.output3']

    ReadData={
        "Pierre":table_names[0],
        "Jacques":table_names[1],
        "Paul":table_names[2],
        "target_table": "smartlive:EtienneResults.table_alex1",
        }

    
    window_duration = 60
    ##allowed_lateness = opts.allowed_lateness
    ##dead_letter_bucket = opts.dead_letter_bucket


    # Table schema for BigQuery
    table_schema = {
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
    }

    def table_fn(data=dest_view):
        embed()
        destination=data[0]#row = json.loads(readData.decode('utf-8'))
        data.pop(0)
        return destination

    p = beam.Pipeline(options=options)


    readData=(p | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(input_topic))
    
    dest_view=beam.pvalue.AsList(
        readData | 'CreateCharacters' >> beam.Map(parse_json2))
    """| "Window into Fixed Intervals" >> beam.WindowInto
        (beam.window.FixedWindows(1),
         trigger=AfterCount(1),
         accumulation_mode=AccumulationMode.DISCARDING))"""

    
    data_view=(readData
            | 'ParseJson' >> beam.Map(parse_json))
    """| "Window2 into Fixed Intervals" >> beam.WindowInto
        (beam.window.FixedWindows(1),
         trigger=AfterCount(1),
         accumulation_mode=AccumulationMode.DISCARDING))"""


    result=(data_view | 'WriteWithDynamicDestination' >> beam.io.WriteToBigQuery(
                table = lambda row, dest_view : table_fn(dest_view),
                schema=table_schema,
                table_side_inputs=(dest_view, ),
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                ))
                 

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
  run()
    

    
