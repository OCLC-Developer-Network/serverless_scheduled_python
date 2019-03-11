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

item_file = "sh_items.txt"
csv.register_dialect('piper', delimiter='|', quoting=csv.QUOTE_NONE)
csv_read = csv.DictReader(open(item_file), dialect="piper");
                        
output = io.StringIO()

fieldnames = ['Institution_Symbol','Item_Holding_Location','Item_Permanent_Shelving_Location','Item_Temporary_Shelving_Location','Item_Type','Item_Call_Number','Item_Enumeration_and_Chronology','Author_Name','Title','Material_Format','OCLC_Number','Item_Barcode','Item_Status_Current_Status','n_callnumber_sort', 'n_callnumber_search', 'classifiation', 'class_letters']

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
        row['n_callnumber_sort'] = normalizedNumber.for_sort()
        row['n_callnumber_search'] = normalizedNumber.for_search()
        row['cn_classification'] = normalizedNumber.classification
        if type(normalizedNumber) is a LC:
            row['cn_class_letters'] = normalizedNumber.classification.letters
    writer.writerow(row)

indexFile = csv.DictReader(io.StringIO(output.getvalue()), dialect="piper")

helpers.bulk(es, indexFile, index='items', doc_type='_doc')

return "success"
             