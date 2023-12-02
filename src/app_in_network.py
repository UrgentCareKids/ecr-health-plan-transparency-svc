import json
import psycopg2
import psycopg2.extras
import ijson
import boto3
import sys
import gzip
import shutil
import os
import pathlib
from pathlib import Path
from ijson.common import ObjectBuilder
from decimal import *
from datetime import datetime

# Get and check for existence the passed parameters
# exit if all 3 are not passed
if sys.argv[0] is None or sys.argv[1] is None or sys.argv[2] is None:
    print(f"Please call with 3 parameters (bucket_name,file_path,file_name)")
    sys.exit()
else:
    bucket_name = sys.argv[1]
    print(f"bucket_name : {bucket_name}")
    file_path = sys.argv[2]
    print(f"file_path : {file_path}")
    file_name = sys.argv[3]
    print(f"file_name : {file_name}")

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        # 👇️ if passed in object is instance of Decimal
        # convert it to a string
        if isinstance(obj, Decimal):
            return str(obj)
        # 👇️ otherwise use the default behavior
        return json.JSONEncoder.default(self, obj)

#Get the db connection
#ssm = boto3.client('ssm',  aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],  region_name='us-east-2')
ssm = boto3.client('ssm')

# local dev ONLY
#param = ssm.get_parameter(Name='db_postgres_local_scott', WithDecryption=True )
param = ssm.get_parameter(Name='db_postgres_transparency_svc', WithDecryption=True )

params_request = json.loads(param['Parameter']['Value']) 

def postgres_conn():
    hostname = params_request['host']
    portno = params_request['port']
    dbname = params_request['database']
    dbusername = params_request['user']
    dbpassword = params_request['password']
    conn = psycopg2.connect(host=hostname,user=dbusername,port=portno,password=dbpassword,dbname=dbname)
    return conn

#Setting auto commit false
postgres_conn.autocommit = True
print(postgres_conn)


#Creating a cursor object using the cursor() method
#cursor = postgres_conn.cursor()
targetconnection = postgres_conn()
cursor = targetconnection.cursor()

# Initialize AWS S3 client
#s3 = boto3.client('s3',  aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],  region_name='us-east-2'))
s3 = boto3.client('s3')
#bucket_name = 'uc4k-db'
#file_name = 'data/public-data-files/transparency-index/2023-08-01_anthem_index.json'
local_path = '/mount/datastorage'
# pull S3 file to local file, if it does not already exist
# local dev ONLY  comment out S3
s3.download_file(bucket_name,file_path+file_name,local_path + '/' + file_name)

# verify file created
#extension = ".gz"
local_file_path = local_path + '/' + file_name
print('local file path = ' + local_file_path)
lfile = pathlib.Path(local_file_path)
path_exists = Path.exists(lfile)

if path_exists:
    print('Local file created : ' + file_name,datetime.now())
    if lfile.suffix == ".gz":       #endswith(extension)
        print('Local file is a gz file')
        # Extract the file name without the '.gz' extension
        file_name = os.path.splitext(os.path.basename(lfile))[0]
        new_file_name = local_path + '/' + file_name
        lfile2 = pathlib.Path(new_file_name)
        with gzip.open(lfile, 'rb') as uncompressed_file:        #lfile or file_name
            with open(f'{new_file_name}', 'wb') as f_out:
                shutil.copyfileobj(uncompressed_file, f_out)    
else:
    print('local file not created!')
    
def in_network(json_filename):
    print('Insurer Index file import into in_network_rate : started : ',datetime.now())
    with open(json_filename, 'rb') as input_file:
        #lot_numbers = ijson.items(input_file, 'in_network.item')
        lot_numbers = ijson.items(input_file, 'provider_references.item')
        #lot_numbers = ijson.items(input_file, 'reporting_structure.item')       
                 
        for dict in lot_numbers:
            
            try:
                df = json.dumps(dict, cls=DecimalEncoder)
                df = df.replace("'","")
                # index
                #query = "insert into reporting_plan(file_nm,json_payload)  values('{}','{}')".format(file_name,df)
                # in network rate
                query = "insert into in_network_rate(file_nm,json_payload)  values('{}','{}')".format(file_name,df)
                # provider mapping
                #query = "insert into in_network_rate_provider(file_nm,json_payload)  values('{}','{}')".format(file_name,df)
                cursor.execute(query)
            except (Exception, psycopg2.Error) as e:
                error_msg = 'ERROR  ' + str(e.pgcode) + " : " + str(e)
                print(error_msg)
                f = open("error.txt", "w")    # w overwrite  a append
                f.write(error_msg + query + '\n')
                f.close()    
                targetconnection.rollback()
                lfile.unlink()
                if lfile.suffix == ".gz":
                    lfile2.unlink()
                break
          
          
    print('Insurer Index file import into in_network_rate : completed : ',datetime.now())
         #   print(dict)
         #print(json.dumps(dict, cls=DecimalEncoder))

in_network(local_path + '/' + file_name)


# for key, value in objects(open('./data/2022-07-01_DIAMONDHEAD-URGENT-CARE-LLC_PS1-50_C2_in-network-rates.json', 'rb')):
#    print(key, ' ', value)
   # cursor.execute("insert into json_blob(json_payload) values(concat(%s, %s))", (key, json.dumps(value)))

# Commit your changes in the database
targetconnection.commit()
print("Records commited........ ",datetime.now())
lfile.unlink()
if lfile.suffix == ".gz":
    lfile2.unlink()

print('Local file deleted : ' + file_name,datetime.now())

# pull it apart
# print('Insert into plan_file_location : started :',datetime.now())
# query = """  insert into plan_file_location(index_file_nm,plan_nm,plan_file_url)
# 	select distinct file_nm as index_file_nm
# 			,jsonb_array_elements(json_payload ->'in_network_files')::jsonb->>('description') as plan_nm
# 		  	,jsonb_array_elements(json_payload ->'in_network_files')::jsonb->>('location') as file_url
# 	  from reporting_plan where file_nm = '{}' ;""".format(file_name)
# cursor.execute(query)
# print('Insert into plan_file_location : completed :',datetime.now())
# targetconnection.commit()
# print("Records commited........ ",datetime.now())

# Closing the connection
targetconnection.close()

