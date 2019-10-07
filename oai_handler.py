import boto3
from elasticsearch import helpers, Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import yaml
from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader

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
    #pull data from OAI endpoint
    registry = MetadataRegistry()
    registry.registerReader('oai_dc', oai_dc_reader)
    url_base = event['siteURL']
    URL = url_base + event['oaiPath']
    client = Client(URL, registry, force_http_get=True)

    harvested_data = [];
    for record in client.listRecords(metadataPrefix='oai_dc'):
        if not record[0].isDeleted():
            fields = record[1].getMap();
            if fields['subject']:
                fields['subjects'] = fields['subject'][0].split(';')
                del fields['subject']
            fields['set'] = record[0].setSpec()
            identifier = record[0].identifier().split(':')[2]
            fields['image_url_base'] = url_base + '/digital/iiif/' + identifier + '/'
            harvested_data.append(fields)
        
    es.indices.delete(index='digital_collection_recs', ignore=[400, 404])
    
    mapping = {
            "mappings":{
                "properties": {
                    "title": {"type": "text"}, 
                    "creator": {"type": "text"},
                    "subjects": {"type": "text"}, 
                    "description": {"type": "text"},
                    "publisher": {"type": "text"},
                    "contributor": {"type": "text"},
                    "date": {"type": "text"},
                    "type": {"type": "text", "fielddata": "true"},
                    "format": {"type": "text", "fielddata": "true"},
                    "identifier": {"type": "text"},
                    "source": {"type": "text"},
                    "language": {"type": "text", "fielddata": "true"},
                    "relation": {"type": "text"},
                    "coverage": {"type": "text"},
                    "rights": {"type": "text"},
                    "set": {"type": "text", "fielddata": "true"},
                    "image_url_base": {"type": "text"}
                }
            }
        }                
    es.indices.create(index='digital_collection_recs', body=mapping)
    
    helpers.bulk(es, harvested_data, index='digital_collection_recs', doc_type='_doc')
    
    return "success"
             