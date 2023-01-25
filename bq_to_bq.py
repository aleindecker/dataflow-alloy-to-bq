"""
This will read data from a BQ table, deidentify predefined columns, and then write the deidentified data to BigQuery
Command to run this script:
python main.py --input_table {BQ_PROJECT:BQ_SCHEMA.BQ_TABLE} --output_table {BQ_PROJECT:BQ_SCHEMA.BQ_TABLE} --temp_location {gs://your_temp_bucket} --project_id your_project_id --runner DataflowRunner --job your_job_name 
"""
import sys
import re
import json
import pandas as pd
import os
import setup
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition, BigQuerySource
from apache_beam.typehints.schemas import LogicalType
from apache_beam.ml.gcp.cloud_dlp import MaskDetectedDetails
# Import the client library
import google.cloud.dlp
import google.cloud.dlp_v2
from google.cloud import dlp_v2

def log(row, level="debug"):
    getattr(logging, level.lower())(row)
    return row

#Get credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/root/.config/gcloud/application_default_credentials.json'
print('Credentials from environ: {}'.format(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')))

#Define info_types that DLP API will look for 
info_types = ['PERSON_NAME', 'EMAIL_ADDRESS', 'LOCATION', 'PHONE_NUMBER', 'IP_ADDRESS', 'MAC_ADDRESS' ]

#Set the inspection configuration that the DLP API will use
inspect_config = {
        "info_types": [{"name": info_type} for info_type in info_types]
    }

#Set the deidentification configuration for the DLP API, including masking character
deidentify_config = {
        "info_type_transformations": {
            "transformations": [
                {
                    "primitive_transformation": {
                        "character_mask_config": {
                            "masking_character": '*'
                        }
                    }
                }
            ]
        }
    }

#Converts bytes after running the data through DLP back to JSON in order to write to BQ
def to_json(data):
    d=json.loads(data)
    return d 

#Converts data from postgres to bytes in order to run through the DLP API
def to_bytes(data):
    d=json.dumps(data).encode('utf-8')
    print(d)
    return d


class ParseMessageFn(beam.DoFn):
    def __init__(self):
        super(ParseMessageFn, self).__init__()

    def process(self, elem):
        message = json.loads(elem)
        yield message

#Set up parameters needed
class SQLOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input_table', dest='input_table', required=True, 
                                           help=('Input BigQuery table as source, specified as: PROJECT:DATASET.TABLE '))
        parser.add_value_provider_argument('--project_id', dest='project_id')
        parser.add_value_provider_argument('--username', dest='username')
        parser.add_value_provider_argument('--password', dest='password')
        parser.add_argument('--output_table', dest='output_table', required=True, 
                                           help=('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '))

#Define schema for BQ table
SCHEMA = ",".join(
    [
        "name:STRING",
        "email:STRING",
        "phone:STRING",
        "ip_address:STRING",
    ]
)

#Pipeline
def run():
    pipeline_options = PipelineOptions()
    options = pipeline_options.view_as(SQLOptions)
    with beam.Pipeline(options=options) as p:

        data = (p | BigQuerySource(options.input_table, project = options.project_id)
                | 'to bytes' >> beam.Map(to_bytes)
                | 'dlp' >> MaskDetectedDetails(project='alein-dataflow-prime-prd', 
                                      deidentification_config=deidentify_config, 
                                      inspection_config=inspect_config)
                | 'reformat' >> beam.Map(to_json)
               )
        data | 'Write to Table' >> WriteToBigQuery(
                               options.output_table,
                               schema = SCHEMA,
                               create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                               write_disposition=BigQueryDisposition.WRITE_APPEND 
            #for batch pipelines, the write disposition can be WRITE_TRUNCATE, but must be WRITE_APPEND for stream
                               )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()
