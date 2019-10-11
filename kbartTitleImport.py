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

    csv_read = csv.DictReader(item_file, delimiter="\t");
    
    file_data = []
    for row in csv_read:
        del row['coverage_notes']
        del row['title_notes']
        del row['staff_notes']
        del row['oclc_linkscheme']
        del row['ACTION']
        file_data.append(row)                                                            
    
    es.indices.delete(index='title_list', ignore=[400, 404])
    
    mapping = {
                "mappings":{
                    "properties": {
                        "publication_title": {"type": "keyword"},
                        "print_identifier": {"type": "keyword"},
                        "online_identifier": {"type": "keyword"},
                        "date_first_issue_online": {"type": "keyword"},
                        "num_first_vol_online": {"type": "keyword"},
                        "num_first_issue_online": {"type": "keyword"},
                        "date_last_issue_online": {"type": "keyword"},
                        "num_last_vol_online": {"type": "keyword"},
                        "num_last_issue_online": {"type": "keyword"},
                        "title_url": {"type": "keyword"},
                        "first_author": {"type": "keyword"},
                        "title_id": {"type": "keyword"},
                        "embargo_info": {"type": "keyword"},
                        "coverage_depth": {"type": "keyword"},
                        "publisher_name": {"type": "keyword"},
                        "location": {"type": "keyword"},
                        "vendor_id": {"type": "keyword"},
                        "oclc_collection_name": {"type": "keyword"},
                        "oclc_collection_id": {"type": "keyword"},
                        "oclc_entry_id": {"type": "keyword"},
                        "oclc_number": {"type": "keyword"}
                    }
                }
            }                
    es.indices.create(index='title_list', body=mapping)
    
    helpers.bulk(es, file_data, index='title_list', doc_type='_doc')
    
    return "success"     