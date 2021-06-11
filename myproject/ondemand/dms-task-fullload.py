import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
# import numpy as np
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
client = boto3.client('dms')
response = client.describe_replication_tasks(
    Filters=[
        {
            'Name': 'migration-type',
            'Values': [
                'full-load',
            ]
        },
    ],
    
)
print (response)

for status in response['ReplicationTasks']:
    taskname = status['ReplicationTaskIdentifier']
    # print(status)
    if 'ondemand' in taskname:
        taskarn = status['ReplicationTaskArn']
        riarn = status['ReplicationInstanceArn']
        statusvalue = status['Status']
        repstats = status['ReplicationTaskStats']
        fulloadvalue = repstats['FullLoadProgressPercent']
        print(taskname)
        print(taskarn)
        print (statusvalue)
        print (fulloadvalue)
        file = open("C:/Users/SVS0RMR/Downloads/AssetInventory/ondemand/status.txt", "w")
        file.write("Car Name = " + taskarn + "\n" +"Car Year = "+riarn )
        file.close
# s3 = boto3.client('s3')
# file_path = 'C:\Users\SVS0RMR\Downloads\AssetInventory\ondemand\'+'status.txt'
# bucket_name = '2env-en411-edp-cloudformation-s3'
# bucket_folder = 'dms/ondemand'
# # bucket_folder2 = 'assetinventory/assetinventory-history'
# dest_file_name = 'status.txt'
# # dest_file_name2 = datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%p")+'_'+'assetinventory-nonprod.csv'
# s3.upload_file(file_path,bucket_name, '%s/%s' % (bucket_folder,dest_file_name),ExtraArgs={'ACL': 'bucket-owner-full-control'})
# s3.upload_file(file_path,bucket_name, '%s/%s' % (bucket_folder2,dest_file_name2),ExtraArgs={'ACL': 'bucket-owner-full-control'})
        # if statusvalue == 'running' or statusvalue == 'stopped' :
        if fulloadvalue >= 100 :
            
            response2 = client.stop_replication_task(
                ReplicationTaskArn= taskarn
            )
            response = client.delete_replication_instance(
                ReplicationInstanceArn= riarn
            )
