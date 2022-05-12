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

class CommonLog(typing.NamedTuple):
    name: str
    age: int
    height: float

beam.coders.registry.register_coder(CommonLog, beam.coders.RowCoder)



def parse_json(element):
    #embed()
    row = json.loads(element.decode('utf-8')
                     )
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
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('etienne-streaming-three-roads-',time.time_ns())
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    input_topics = ['projects/smartlive/topics/my_topic1',
                    'projects/smartlive/topics/my_topic2',
                    'projects/smartlive/topics/my_topic3']
    
    table_names = ['EtienneResults.output1',
                   'EtienneResults.output2',
                   'EtienneResults.output3']
    
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

    p = beam.Pipeline(options=options)



    road1 = (p | 'ReadFromPubSub1' >> beam.io.ReadFromPubSub(input_topics[0])
                  | 'ParseJson1' >> beam.Map(parse_json)#.with_output_types(CommonLog)
                  | 'WriteAggToBQ1' >> beam.io.WriteToBigQuery(
                      table_names[0],
                schema=table_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )                                                   #.with_output_types(CommonLog)
                 )

    road2 = (p | 'ReadFromPubSub2' >> beam.io.ReadFromPubSub(input_topics[1])
                  | 'ParseJson2' >> beam.Map(parse_json)#.with_output_types(CommonLog)
                  | 'WriteAggToBQ2' >> beam.io.WriteToBigQuery(
                table_names[1],
                schema=table_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )                                                   #.with_output_types(CommonLog)
                 )

    road3 = (p | 'ReadFromPubSub3' >> beam.io.ReadFromPubSub(input_topics[2])
                  | 'ParseJson3' >> beam.Map(parse_json)#.with_output_types(CommonLog)
                  | 'WriteAggToBQ3' >> beam.io.WriteToBigQuery(
                table_names[2],
                schema=table_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )                                                   #.with_output_types(CommonLog)
                 )
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()#.wait_until_finish()

if __name__ == '__main__':
  run()
    

    
