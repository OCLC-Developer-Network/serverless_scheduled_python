import boto3
import csv
from elasticsearch import helpers, Elasticsearch, RequestsHttpConnection
import io
from requests_aws4auth import AWS4Auth
import yaml

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
    item_file = response['Body'].read().decode('utf-8').split()    

    csv.register_dialect('semicolon', delimiter=';', quoting=csv.QUOTE_ALL)
    csv_read = csv.DictReader(item_file, dialect="semicolon");                                                            
    
    es.indices.delete(index='circ_events', ignore=[400, 404])
    
    mapping = {
                "mappings":{
                    "properties": {                        
                        "Event Branch Name": {"type": "text", "fielddata": "true"},
                        "Event Institution Name": {"type": "text"},
                        "Event Type": {"type": "text", "fielddata": "true"},
                        "Event Borrower Category": {"type": "text"},
                        "Event Day Name of Week": {"type": "text"},
                        "Event Loan Policy": {"type": "text"},                            
                        "Item Barcode": {"type": "text", "fielddata": "true"},
                        "Item OCLC Number": {"type": "text", "fielddata": "true"},
                        "Item Title": {"type": "text"},
                        "Item Call Number": {"type": "text"},
                        "Item Material Format": {"type": "text", "fielddata": "true"},    
                        "Item Branch Name": {"type": "text", "fielddata": "true"},
                        "Item Permanent Shelving Location": {"type": "text", "fielddata": "true"},
                        "Item Renewal Count":{"type": "integer"},
                        "Patron Barcode": {"type": "text", "fielddata": "true"},
                        "Patron Custom Category 1": {"type": "text", "fielddata": "true"},
                        "Patron Custom Category 2": {"type": "text", "fielddata": "true"},
                        "Patron Custom Category 3": {"type": "text", "fielddata": "true"},
                        "Patron Custom Category 4": {"type": "text", "fielddata": "true"},
                        "Event Staff Full Name": {"type": "text", "fielddata": "true"},
                        "Item Temporary Shelving Location": {"type": "text","fielddata": "true"},                    
                        "Event Date": {"type": "date", "format": "yyyy/MM/dd HH:mm:ss||yyyy/MM/dd"},
                        "Event Date/Time": {"type": "date", "format": "yyyy/MM/dd HH:mm:ss||yyyy/MM/dd"},
                        "Event Due Date/Time": {"type": "date", "format": "yyyy/MM/dd HH:mm:ss||yyyy/MM/dd"}
                    }
                }
            }                
    es.indices.create(index='circ_events', body=mapping)
    
    helpers.bulk(es, csv_read, index='circ_events', doc_type='_doc')
    
    return "success"     