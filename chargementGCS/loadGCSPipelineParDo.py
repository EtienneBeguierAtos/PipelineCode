import argparse
import time
import logging
import json
import typing
from datetime import datetime
from constant import tables, table_schema2, table_schema
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AfterProcessingTime, AfterAll, AfterAny
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.runners import DataflowRunner, DirectRunner
from google.cloud import storage
from google.cloud import bigquery
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
# ### functions and classes

def getSchema(element):
    names=element["address"].split("/")
    if not names[-1] in tables:
        return tables["tabledata.csv"]
    return tables[names[-1]]


def getMessage(filename, messages):
    for message in messages:
        if message["address"].equals(filename):
            return message
    return


def parse_json(element):
    #embed()
    row = json.loads(element.decode('utf-8'))
    #embed()
    return row #CommonLog(**row)

def partition_fn(elements, num):
    #embed()
    for element in elements:
        return element


def parse_csv(element, message):
    lines=element.split(",")
    #head=lines[0]
    #lines.pop(0)
    #schema=tables["tabledata.csv"]["fields"]
    #names=message["address"].split("/")
    #if names[-1] in tables:
    #    schema= tables[names[-1]]["fields"]

    schema=getSchema(message[0])["fields"]
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

class GetTimestamp(beam.DoFn):
  def process(self, plant, timestamp=beam.DoFn.TimestampParam):
    event_id = int(timestamp.micros / 1e6)



class ReadCsv(beam.DoFn):
    def start_bundle(self):
        self.storage_client = storage.Client()
    
    def process(self, message):
        address=message["address"].split("/")
        bucket = self.storage_client.get_bucket(address[2])
        blob = bucket.get_blob(address[3]+"/"+address[4])
        downloaded_blob = blob.download_as_string().decode('utf-8')
        lines=downloaded_blob.split("\r\n")

        line_count = 0
        schema=[]
        list_dict=[]
        #embed()
        for line in lines:
            #embed()
            dict={}
            splitted_line=line.split(",")
            if line_count==0:
                schema=splitted_line
                line_count+=1
            else:
                for col in range(len(schema)):
                    dict[schema[col]]=splitted_line[col]
                list_dict.append(dict)
        yield [list_dict,message]




# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into a dynamic BigQuery output')


    # Setting up the Beam pipeline options
    options = PipelineOptions( save_main_session=True, streaming=True)
    options.view_as(GoogleCloudOptions).project = 'smartlive'
    options.view_as(GoogleCloudOptions).region = 'europe-west1'
    options.view_as(GoogleCloudOptions).staging_location = 'gs://testinsertbigquery/staging'
    options.view_as(GoogleCloudOptions).temp_location = 'gs://testinsertbigquery/temp'
    options.view_as(GoogleCloudOptions).dataflow_service_options=["enable_prime"]
    options.view_as(DebugOptions).experiments=["use_runner_v2"]
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('chargement-fichier-streaming-primerunnerv2',time.time_ns())
    options.view_as(SetupOptions).requirements_file="requirements.txt"
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    input_topic = 'projects/smartlive/topics/my_topic3'

    dataset='EtienneResults'


    

    def table_fn(element):
        #embed()
        destination=element["destination"]
        return options.view_as(GoogleCloudOptions).project + "." + dataset + "."+ destination

    

    class InsertCsv(beam.DoFn):
        def start_bundle(self):
            self.bigquery_client = bigquery.Client()

        def process(self, pair):
            dicts=pair[0]
            message=pair[1]
            table_id=table_fn(message)
            tables = self.bigquery_client.list_tables(options.view_as(GoogleCloudOptions).project + "." + dataset)
            list_id=[]
            for table in tables:
                list_id.append(table.table_id)
            if message["destination"] not in list_id:
                table = bigquery.Table(table_id, schema=getSchema(message)["fields"])
                table = self.bigquery_client.create_table(table)

            self.bigquery_client.insert_rows_json(table_id, dicts)
    
    window_duration = 60
    ##allowed_lateness = opts.allowed_lateness
    ##dead_letter_bucket = opts.dead_letter_bucket



    p = beam.Pipeline(options=options)


    readData=(p 
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(input_topic)
            | 'ParseJson' >> beam.Map(parse_json)
            #| 'With timestamps' >> beam.Map(
            #    lambda message: beam.window.TimestampedValue(message, time.time_ns()))
            )

    #partData=(readData | beam.Partition(partition_fn, 2))

    #dest_view=beam.pvalue.AsList(
        #readData )
        #| "Window2 into Fixed Intervals" >> beam.WindowInto
        #(beam.window.GlobalWindows(),
        # trigger=AfterAny(
        #                  AfterCount(1),
        #                  AfterProcessingTime(1)),
        # accumulation_mode=AccumulationMode.DISCARDING))

    #dest_view | 'Get timestamp' >> beam.ParDo(GetTimestamp())

    
    data_view=(readData
            | 'Read CSV files' >> beam.ParDo(ReadCsv()))
            #| 'Get CSV address' >> beam.Map(getURI)
            #| 'Read CSV files' >> beam.io.ReadAllFromText(skip_header_lines=1))
            #| beam.GroupBy(lambda s: 0)
            #| 'ParseCSV' >> beam.Map(parse_csv)
            #| "Window3 into Fixed Intervals" >> beam.WindowInto
        #(beam.window.FixedWindows(1),
        # trigger=AfterProcessingTime(1),
        # accumulation_mode=AccumulationMode.DISCARDING))

    data_view | 'WriteWithDynamicDestination' >> beam.ParDo(InsertCsv())

    #(data_view
    #    | 'ParseCSV' >> beam.Map(parse_csv, message=dest_view)
    #    | 'WriteWithDynamicDestination' >> beam.io.WriteToBigQuery(
    #            table=lambda row, dest_view : table_fn(row,dest_view[0]),
    #            schema=lambda row, dest_view : getSchema(dest_view[0]),
    #            table_side_inputs=(dest_view, ),
    #            schema_side_inputs=(dest_view, ),
    #            #additional_bq_parameters={'ignoreUnknownValues': True},
    #            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    #            ))
                 

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
  run()
    

    
