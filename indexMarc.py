import boto3
import csv
from elasticsearch import helpers, Elasticsearch, RequestsHttpConnection
import io
import pycallnumber as pycn
from requests_aws4auth import AWS4Auth
import yaml
from pymarc import parse_xml_to_array
from nntplib import subject

credentials = boto3.Session().get_credentials()

s3 = boto3.client('s3')
    
# read a configuration file
with open("prod_config.yml", 'r') as stream:
    config = yaml.load(stream)

awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, config.get('region'), config.get('service'))
    
es = Elasticsearch(
    hosts = [{'host': config.get('eshost'), 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

def run(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    # need to get the file from S3
    response = s3.get_object(Bucket=bucket, Key=key)  
    item_file = io.StringIO(response['Body'].read().decode('utf-8'))
    # read data in pymarc
    records = parse_xml_to_array(item_file)
    file_data = []    
    for record in records:
        contributors = list(map(lambda contributor: contributor.value(), record.addedentries()))        
        non_fast_subjects = filter(lambda subject: subject.indicator2 != "7",  record.subjects())     
        subjects = list(map(lambda subject: subject.format_field(), non_fast_subjects))
        fast_subjects = filter(lambda subject: subject.indicator2 != "7",  record.subjects())     
        fast_headings = list(map(lambda subject: subject.get_subfields('a', 'x'), fast_subjects))
        notes = list(map(lambda note: note.value(), record.notes()))
        physicaldescription = list(map(lambda field: field.value(), record.physicaldescription()))
        if record['050']:
            lc_callnums = list(map(lambda cn: cn.format_field(), record.get_fields('050')))
        else:
            lc_callnum = ""
        if record['082']:
            dewey_callnum = list(map(lambda cn: cn.format_field(), record.get_fields('082')))
        else:
            dewey_callnum = ""
        
        metadata = {
            #"raw_marc": record.as_json(),
            "oclcnumber": record.get_fields('001')[0].value(),
            "title" : record.title(),
            "author": record.author(),
            "contributors": contributors,
            "isbns": record.isbn(),
            "subjects": subjects,
            "fast_subjects": fast_headings,
            "notes": notes,
            "physicaldescription": physicaldescription,
            "publisher": record.publisher(), 
            "pubyear": record.pubyear(),
            "lc_callnum": lc_callnum,
            "dewey_callnum": dewey_callnum
        }
        file_data.append(metadata)
        
    es.indices.delete(index='bib_items', ignore=[400, 404])
    
    mapping = {
            "mappings":{
                "properties": {
                    "author": {"type": "keyword"},
                    "title": {"type": "text"},
                    "oclcnumber": {"type": "keyword"},
                    "notes": {"type: text"},
                    "physicaldescription": {"type": "text"},
                    "publisher": {"type": "keyword"},
                    "pubyear": {"type": "date", "format": "Y", "ignore_malformed": "true"},
                    "lc_callnum": {"type": "keyword"},
                    "dewey_callnum": {"type": "keyword"}
                }
            }
        }                
    es.indices.create(index='bib_items', body=mapping)
    
    helpers.bulk(es, file_data, index='bib_items', doc_type='_doc')
    
    return "success" 
