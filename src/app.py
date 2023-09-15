import json
import psycopg2
import psycopg2.extras
import ijson
import boto3
from ijson.common import ObjectBuilder
from decimal import *
from datetime import datetime

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
# local dev
param = ssm.get_parameter(Name='db_postgres_local_scott', WithDecryption=True )
#param = ssm.get_parameter(Name='db_postgres_transparency_svc', WithDecryption=True )
params_request = json.loads(param['Parameter']['Value']) 

def postgres_conn():
    hostname = params_request['host']
    portno = params_request['port']
    dbname = params_request['database']
    dbusername = params_request['user']
    dbpassword = params_request['password']
    conn = psycopg2.connect(host=hostname,user=dbusername,port=portno,password=dbpassword,dbname=dbname)
    return conn

#Establishing the connection
#conn = psycopg2.connect(
#   database="lab", user='pguser', password='calvin', host='127.0.0.1', port= '15435'
#)

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
bucket_name = 'uc4k-db'
file_name = 'data/public-data-files/transparency-index/2023-08-01_anthem_index.json'
local_path = '/mount/datastorage'
# pull S3 file to local file
s3.download_file(bucket_name,file_name,local_path + '/' + '2023-08-01_anthem_index.json')
print('Local file created : ' + file_name,datetime.now())
in_network = (local_path + '/' + '2023-08-01_anthem_index.json')

#in_network("../../gsh_transparency_parser_python/data/2023-08-01_anthem_index.json")
#2 NO
# S3   do i have to download?  s3.download_file(bucket_name,file_key,what do i want to name it ie. in_network)
#s3_obj = s3.get_object(Bucket='uc4k-db', Key='/data/public-data-files/transparency-index/2023-08-01_anthem_index.json')
#in_network = s3_obj['Body'].read()
# 2  NO
# 3 NO
#def read_file_from_s3(bucket_name, file_name):
#    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
#    data = obj['Body'].read()
#    return data
# 3 NO
# https://stackoverflow.com/questions/70913017/how-to-read-content-of-a-file-from-a-folder-in-s3-bucket-using-python
# 4
#response = s3.get_object(Bucket=bucket_name, Key=file_name)
#bytes = response['Body'].read()  # returns bytes since Python 3.6+
#in_network = json.loads(bytes['Body'].read().decode('utf-8')) #original got error
#in_network = json.loads(str['Body'].read().decode('utf-8'))
#  error text
    #File "/mnt/c/Users/smaxw/OneDrive/work/UrgentCare4Kids/Git/ecr-health-plan-transparency-svc/src/app.py", line 73, in <module>
    #in_network = json.loads(bytes['Body'].read().decode('utf-8'))
    #TypeError: byte indices must be integers or slices, not str
#  error text

def in_network(json_filename):
    print('Starting in_network import. ',datetime.now())
    with open(json_filename, 'rb') as input_file:
        #lot_numbers = ijson.items(input_file, 'in_network.item')
        lot_numbers = ijson.items(input_file, 'reporting_structure.item')        
        for dict in lot_numbers:
            
            try:
                df = json.dumps(dict, cls=DecimalEncoder)
                df = df.replace("'","")
                query = "insert into reporting_plan(file_nm,json_payload)  values('{}','{}')".format(json_filename,df)
                #query = "insert into in_network_file(json_payload)  values('{}')".format(df)                
                cursor.execute(query)
            except (Exception, psycopg2.Error) as e:
                error_msg = 'ERROR  ' + str(e.pgcode) + " : " + str(e)
                print(error_msg)
                f = open("error.txt", "w")    # w overwrite  a append
                f.write(error_msg + query + '\n')
                f.close()    
                targetconnection.rollback()
                break
          
          
    print('in_network complete. ',datetime.now())
         #   print(dict)
         #print(json.dumps(dict, cls=DecimalEncoder))

# for key, value in objects(open('./data/2022-07-01_DIAMONDHEAD-URGENT-CARE-LLC_PS1-50_C2_in-network-rates.json', 'rb')):
#    print(key, ' ', value)
   # cursor.execute("insert into json_blob(json_payload) values(concat(%s, %s))", (key, json.dumps(value)))

# Commit your changes in the database
targetconnection.commit()
print("Records commited........ ",datetime.now())

# Closing the connection
targetconnection.close()

