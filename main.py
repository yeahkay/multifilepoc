import ast
import io
import json
from io import BytesIO
from urllib.parse import quote_plus

import boto3
import pandas as pd
from sqlalchemy import create_engine

content = open('config.json')
config = json.load(content)
access_key = config['access_key']
secret_access_key = config['secret_access_key']
table_notinclude = config['notinclude_table']
pwd = "!QAZ1qaz"
user = "root"
host = "localhost"
db = "dev"
port = 3306

s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,
                  region_name='us-east-1')
f = BytesIO()
s3.download_fileobj("poc-practice-bucket-1", "newlistdone.txt", f)
res = ast.literal_eval(str(f.getvalue().decode()))
print(table_notinclude)

res.extend(ast.literal_eval(table_notinclude))

exclude = str(res).strip("[]")
print(res)

def extract():
    try:
        # engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}/{db}?host={host}?port=3306")
        # connection = engine.connect()
        engine= create_engine("mysql+pymysql://root:%s@localhost/dev"% quote_plus("!QAZ1qaz"))
        connection = engine.connect()
        print(connection)

        # connection = mysql.connector.connect(
        #     host="localhost",
        #     port="3306",
        #     user="root",
        #     passwd="!QAZ1qaz",
        #     database="dev"
        # )
        # src_tables = connection.execute(""" show tables""")

        # s3 = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,region_name='us-east-1')
        # resp = s3.put_object(
        #     Bucket="poc-practice-bucket-1", Key="donelist.txt", Body="['test2','authors']")
        # print("s3 data",resp)
        # obj = s3.Bucket("poc-practice-bucket-1","donelist.txt")



        src_tables = connection.execute(f""" SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_type = 'BASE TABLE' and table_schema="{db}" and table_name not in ({exclude})""")
        for tbl in src_tables:
            df = pd.read_sql_query(f'select * FROM {tbl[0]}', engine)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error: " + str(e))


def load(df, tbl):
    try:
        upload_file_bucket = 'poc-practice-bucket-1'
        upload_file_key = 'test/' + str(tbl) + f"/{str(tbl)}"
        filepath =  upload_file_key + ".csv"
        #
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,region_name='us-east-1')

        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)

            response = s3_client.put_object(
                Bucket=upload_file_bucket, Key=filepath, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                res.append(tbl)
                print(f"{tbl} S3 Data imported successfully - Status - {status}")
            else:
                print(f"Unsuccessful S3 Data imported. Status - {status}")
        print(res)
        resp = s3.put_object(
            Bucket="poc-practice-bucket-1", Key="newlistdone.txt", Body=str(res))
        status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
        print(status)
    except Exception as e:
        print("Data load error: " + str(e))

try:
    extract()
    print("All Data imported")
except Exception as e:
    print("Error while extracting data: " + str(e))