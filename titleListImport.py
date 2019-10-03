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
    item_file = response['Body'].read().decode('utf-8').split('\n')    

    csv.register_dialect('semicolon', delimiter=';', quoting=csv.QUOTE_ALL)
    csv_read = csv.DictReader(item_file, dialect="semicolon");                                                            
    
    file_data = []
    for row in csv_read:
        del row['New Titles']
        file_data.append(row)
    
    es.indices.delete(index='new_title_list', ignore=[400, 404])
    
    mapping = {
                "mappings":{
                    "properties": { 
                        "Publication Date": {"type": "text", "fielddata": "true"},
                        "Local Item Call Number": {"type": "text", "fielddata": "true"},
                        "Source Application": {"type": "text", "fielddata": "true"},
                        "Institution Name": {"type": "text", "fielddata": "true"},
                        "Author Name": {"type": "text", "fielddata": "true"},
                        "Title": {"type": "text", "fielddata": "true"},
                        "Publisher Name": {"type": "text", "fielddata": "true"},
                        "Edition": {"type": "text", "fielddata": "true"},
                        "Language Name": {"type": "text", "fielddata": "true"},
                        "Material Format": {"type": "text", "fielddata": "true"},
                        "Material Subformat": {"type": "text", "fielddata": "true"},
                        "Local Item Permanent Shelving Location": {"type": "text", "fielddata": "true"},
                        "OCLC Number": {"type": "text", "fielddata": "true"},
                        "Calendar Date": {"type": "date", "format": "yyyy/MM/dd HH:mm:ss||yyyy/MM/dd"}
                    }
                }
            }                
    es.indices.create(index='new_title_list', body=mapping)
    
    helpers.bulk(es, file_data, index='new_title_list', doc_type='_doc')
    
    return "success"     