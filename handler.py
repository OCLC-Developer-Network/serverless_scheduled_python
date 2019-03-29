import boto3
import csv
from elasticsearch import helpers, Elasticsearch, RequestsHttpConnection
import io
import pycallnumber as pycn
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
    item_file = response['Body'].read().decode('utf-8')
    csv.register_dialect('piper', delimiter='|', quoting=csv.QUOTE_NONE)
    csv_read = csv.DictReader(io.StringIO(item_file), dialect="piper");
    
    file_data = []    
    for row in csv_read:
        del row['LHR_Item_Materials_Specified']
        del row['Title_ISBN']
        del row['LHR_Item_Cost']
        del row['LHR_Item_Nonpublic_Note']
        del row['LHR_Item_Public_Note']
        del row['Item_Due_Date']
        del row['Item_Issued_Count']
        del row['Issued_Count_YTD']
        del row['Item_Soft_Issued_Count']
        del row['Item_Soft_Issued_Count_YTD']
        del row['Item_Last_Issued_Date']
        del row['Item_Last_Inventoried_Date']
        del row['Item_Deleted_Date']
        del row['LHR_Date_Entered_on_File']
        del row['LHR_Item_Acquired_Date']
        del row['Language_Code']
        # loop through and normalize call number
        if row['Item_Call_Number']:
            normalizedNumber = pycn.callnumber(row['Item_Call_Number'])
            row['cn_type'] = normalizedNumber.__class__.__name__
            try:
                row['cn_classification'] = str(normalizedNumber.classification)
            except AttributeError:
                row['cn_classification'] = ""
            if isinstance(normalizedNumber, pycn.units.LC):
                try:
                    row['cn_class_letters'] = str(normalizedNumber.classification.letters)
                except AttributeError:
                    row['cn_class_letters'] = ""    
            row['n_callnumber_sort'] = normalizedNumber.for_sort()
            row['n_callnumber_search'] = normalizedNumber.for_search()
        # convert null Publication_Date to null  
        if not row['Publication_Date']:
            row['Publication_Date'] = None
        if row['Publication_Date'] == 0:
            row['Publication_Date'] = None                  
        file_data.append(row) 
        
    es.indices.delete(index='items', ignore=[400, 404])
    
    mapping = {
            "mappings":{
                "_doc":{
                    "properties": {
                        "Institution_Symbol": {"type": "text"},
                        "Item_Holding_Location": {"type": "text", "fielddata": "true"},
                        "Item_Permanent_Shelving_Location": {"type": "text", "fielddata": "true"},
                        "Item_Temporary_Shelving_Location": {"type": "text", "fielddata": "true"},
                        "Item_Type": {"type": "text"},
                        "Item_Call_Number": {"type": "text"},                            
                        "Item_Enumeration_and_Chronology": {"type": "text"},
                        "Author_Name": {"type": "text"},
                        "Title": {"type": "text"},
                        "Material_Format": {"type": "text", "fielddata": "true"},
                        "OCLC_Number": {"type": "text"},
                        "Item_Barcode": {"type": "text"},
                        "Item_Status_Current_Status": {"type": "text", "fielddata": "true"},
                        "n_callnumber_sort": {"type": "text", "fielddata": "true"},
                        "n_callnumber_search": {"type": "text","fielddata": "true"},
                        "cn_classification": {"type": "text","fielddata": "true"},
                        "cn_class_letters": {"type": "text","fielddata": "true"}, 
                        "cn_type": {"type": "text","fielddata": "true"},
                        "Publication_Date": {"type": "date", "format": "Y", "ignore_malformed": "true"}
                    }
                }
            }
        }                
    es.indices.create(index='items', body=mapping)
    
    helpers.bulk(es, file_data, index='items', doc_type='_doc')
    
    return "success"
             