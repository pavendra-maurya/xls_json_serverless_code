import xlrd
import requests
import os
import json
import boto3
from botocore.client import Config

class Download():
    def __init__(self,url,file):
        self.excel_url = url
        self.file_path = file

    def download_start(self):

        response = requests.get(self.excel_url,verify=None)
        with open(self.file_path, 'wb') as data:
            data.write(response.content)
            data.close()

class ConvertXlsToList():
    def __init__(self,file_name):
        self.file_path=file_name

    def getListData(self):
        workbook = xlrd.open_workbook(self.file_path)
        book_sheet = workbook.sheet_by_name("MICs List by CC")
        number_rows,number_cols = book_sheet.nrows,book_sheet.ncols
        excel_header = [book_sheet.cell_value(rowx=0, colx=col) for col in range(number_cols)]
        result_list = []
        for row in range(1, number_rows):
            dict = {}
            for col in range(number_cols):
                dict[excel_header[col]] = book_sheet.cell_value(rowx=row, colx=col)
            result_list.append(dict)
        return result_list

class UploadDataS3():

    def __init__(self,Aws_access_key_id,Aws_secret_access_key):
        self.Aws_access_key_id=Aws_access_key_id
        self.Aws_secret_access_key=Aws_secret_access_key

    def upload_data_S3_start(self,data,BUCKET_NAME,FILE_NAME):
        s3 = boto3.resource(
            's3',
            aws_access_key_id=self.Aws_access_key_id,
            aws_secret_access_key=self.Aws_secret_access_key,
            config=Config(signature_version='s3v4')
        )
        s3.Bucket(BUCKET_NAME).put_object(Key=FILE_NAME, Body=data, ACL='public-read')
        print("Data is uploaded into S3 bucket successfully!")


#...........................launch script into aws lamda..................................

def lambda_handler(event, context):
    Aws_access_key_id = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    Aws_secret_access_key = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    BUCKET_NAME = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

    #..........download data URL..................................................
    URL = "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.xls"
    file = os.path.join(os.getcwd(), "ISO10383_MIC.xls")

    #.................Download data into local system.............................
    Download(URL, file).download_start()

    #................Parse the data xml to json object.............................
    data = ConvertXlsToList(file).getListData()


    #........................upload data into s3 bucket...........................
    upload_data_s3 = UploadDataS3(Aws_access_key_id, Aws_secret_access_key)
    upload_data_s3.upload_data_S3_start(json.dumps(data),BUCKET_NAME ,'ISO10383_MIC.json')

    return "Script is executed successfully"



"""
For Running this script into AWS lambda, just install all python dependencies.
1.  xlrd
2.  requests
3.  boto3

Once installed all dependencies just copy and paste the code into aws lambda file and put all below required credential to run script
1.  Aws_access_key_id
2.  Aws_secret_access_key
3.  BUCKET_NAME
"""
