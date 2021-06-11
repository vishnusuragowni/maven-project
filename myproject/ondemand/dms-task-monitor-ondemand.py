#importing the required libraries for the code
import boto3
import time
from botocore.exceptions import ClientError
from datetime import datetime
client = boto3.client('dms')
def lambda_handler(event, context):
    # boto3 api call to describe the replication task and assigning the metadata to the response variable
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
    # for loop to get the variables from the json metadata
    for status in response['ReplicationTasks']:
        taskname = status['ReplicationTaskIdentifier']
        # print(status)
        # condition to filter ondemand string from the taskname
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
            # condition to look out for fulload percentage
            if fulloadvalue >= 100 :
                
                # response2 = client.stop_replication_task(
                #     ReplicationTaskArn= taskarn
                # )
                # time.sleep(60)
                # boto3 api call to delete the replication task
                response3 = client.delete_replication_task(
                    ReplicationTaskArn= taskarn
                )
                # waiting for the replication task api call to complete the deletion
                time.sleep(120)
                # boto3 api call to delete the replication instance
                response4 = client.delete_replication_instance(
                    ReplicationInstanceArn= riarn
                )