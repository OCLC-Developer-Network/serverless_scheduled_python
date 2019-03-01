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
region = 'us-east-1' # e.g. us-west-1

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

    fieldnames = ['Institution_Symbol','Item_Holding_Location','Item_Permanent_Shelving_Location','Item_Temporary_Shelving_Location','Item_Type','Item_Call_Number','Item_Enumeration_and_Chronology','Author_Name','Title','Material_Format','OCLC_Number','Item_Barcode','Item_Status_Current_Status','n_callnumber']
    
    writer = csv.DictWriter(output, fieldnames=fieldnames, dialect="piper", escapechar='\\')

    writer.writeheader()

    for row in csv_read:
        del row['LHR_Item_Materials_Specified']
        del row['Title_ISBN']
        del row['Publication_Date']
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
            row['n_callnumber'] = normalizedNumber
        writer.writerow(row)
    
    #helpers.bulk(es, csv.DictReader(output), index='items', doc_type='_doc')
    
    
    #bucket = "sftp-content"
    #filename = "updated_item_list.csv"
             
    # write to S3
    #s3.put_object(Body=output.getvalue(), Bucket=bucket, Key=filename)           