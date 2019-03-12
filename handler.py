import boto3
import csv
from datetime import datetime
from elasticsearch import helpers, Elasticsearch, RequestsHttpConnection
import io
from pathlib import Path
import pycallnumber as pycn
import pysftp
from requests_aws4auth import AWS4Auth
import yaml

eshost = 'search-test-item-es-instance-aeewahpkldqvswqovzprrio7py.us-east-1.es.amazonaws.com' # For example, my-test-domain.us-east-1.es.amazonaws.com
region = 'us-east-1' # e.g. us-east-1

service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)

es = Elasticsearch(
    hosts = [{'host': eshost, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

client = boto3.client('kms')
s3 = boto3.client('s3')

def run(event, context):
    eventDate = datetime.strptime(event['time'], '%Y-%m-%dT%H:%M:%SZ');
    reportDate = f"{eventDate:%Y}" + f"{eventDate:%m}" + f"{eventDate:%d}"
    # decrypt configuration file
    data = client.decrypt(CiphertextBlob=Path('prod_config_encrypted.txt').read_bytes())    
    # read a configuration file
    config = yaml.load(data['Plaintext'].decode('utf-8'));
    
    host = config.get('host')
    username = config.get("username")
    key = "id_rsa"
    
    flo = io.BytesIO()
    
    with pysftp.Connection(host, username=username, private_key=key) as sftp:        
           sftp.getfo('wms/reports/OCPSB.Circulation_Item_Inventories.' + reportDate +  '.txt', flo)
    sftp.close()
    
    item_file = flo.getvalue().decode("utf-8");
    csv.register_dialect('piper', delimiter='|', quoting=csv.QUOTE_NONE)
    csv_read = csv.DictReader(io.StringIO(item_file), dialect="piper");
                            
    output = io.StringIO()

    fieldnames = ['Institution_Symbol','Item_Holding_Location','Item_Permanent_Shelving_Location','Item_Temporary_Shelving_Location','Item_Type','Item_Call_Number','Item_Enumeration_and_Chronology','Author_Name','Title', 'Publication_Date', 'Material_Format','OCLC_Number','Item_Barcode','Item_Status_Current_Status','cn_type', 'n_callnumber_sort', 'n_callnumber_search', 'cn_classification', 'cn_class_letters']
    
    writer = csv.DictWriter(output, fieldnames=fieldnames, dialect="piper", escapechar='\\')

    writer.writeheader()

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
                row['cn_classification'] = normalizedNumber.classification
            except AttributeError:
                row['cn_classification'] = ""
            if isinstance(normalizedNumber, pycn.units.LC):
                try:
                    row['cn_class_letters'] = normalizedNumber.classification.letters
                except AttributeError:
                    row['cn_class_letters'] = ""    
            row['n_callnumber_sort'] = normalizedNumber.for_sort()
            row['n_callnumber_search'] = normalizedNumber.for_search()
            # convert null Publication_Date to null    
        writer.writerow(row)

    indexFile = csv.DictReader(io.StringIO(output.getvalue()), dialect="piper")
    
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
                        "Publication_Date": {"type": "date", "format": "Y"}
                    }
                }
            }
        }                
    es.indices.create(index='items', body=mapping)
    
    helpers.bulk(es, indexFile, index='items', doc_type='_doc')
    
    #es.indices.put_mapping(doc_type="_doc", body={"properties": {"n_callnumber_sort": {"type": "text", "fielddata": "true"},"n_callnumber_search": {"type": "text","fielddata": "true"},"cn_classification": {"type": "text","fielddata": "true"},"cn_class_letters": {"type": "text","fielddata": "true"}}, "cn_type": {"type": "text","fielddata": "true"}}}, index=['items'])
    
    return "success"
             