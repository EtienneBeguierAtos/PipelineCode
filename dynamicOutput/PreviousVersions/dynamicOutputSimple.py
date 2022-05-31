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
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AfterProcessingTime
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.runners import DataflowRunner, DirectRunner
#from IPython import embed

# ### functions and classes



def parse_json(element):
    row = json.loads(element.decode('utf-8'))
    #embed()
    return row #CommonLog(**row)




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
    options.view_as(GoogleCloudOptions).dataflow_service_options=["enable_prime"]
    options.view_as(DebugOptions).experiments=["use_runner_v2"]
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('dynamic-output-streaming-tableciblededans-primerunnerv2',time.time_ns())
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    input_topic = 'projects/smartlive/topics/my_topic1'

    dataset='EtienneResults'
    
    table_names = ['smartlive:EtienneResults.output1', 'smartlive:EtienneResults.output2', 'smartlive:EtienneResults.output3']


    

    def table_fn(element):
        #embed()
        destination=element["target_table"]
        return options.view_as(GoogleCloudOptions).project + ":" + dataset + "."+ destination
    
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
        },
        {
            "name": "target_table",
            "mode": "NULLABLE",
            "type": "STRING"
        }
        ] 
    }

    p = beam.Pipeline(options=options)


    readData=(p | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(input_topic))

    
    data_view=(readData
            | 'ParseJson' >> beam.Map(parse_json)
            | 'WriteWithDynamicDestination' >> beam.io.WriteToBigQuery(
                table=table_fn,
                schema=table_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                ))
                 

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
  run()
    

    
