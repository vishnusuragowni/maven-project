import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import numpy as np
import os
import boto3
import time
from datetime import datetime

stackname = 'en411-dms-ri-ondemand4-cf'
taskstackname = 'en411-dms-task-ondemand4-cf'
client = boto3.client('cloudformation')
client2 = boto3.client('dms')
riarn = 'arn:aws:dms:us-east-1:288355943657:rep:JOPZHIB3S55U7P43RL46DR2K6FOJPPMGABKXNAY'
# sourceendpointarn = 'arn:aws:dms:us-east-1:288355943657:endpoint:B7XYY4UPDLXG24XMTNMZLI2OTP35VEW5OLTSXHQ'
# targetendpointarn = 'arn:aws:dms:us-east-1:288355943657:endpoint:KZJWWWEFEV6T7DYQFXCSMUM7FEF32N26CSG3DWY'
response5 = client.describe_stacks(
    StackName= taskstackname
    )
for taskmetadata in response5['Stacks']:
    for taskdeatils in taskmetadata['Outputs']:
        if taskdeatils["OutputKey"] == 'ReplicationTaskName':
            taskid = taskdeatils["OutputValue"]
            print(taskid)
            response6 = client2.describe_replication_tasks(
                Filters=[
                    {
                        'Name': 'replication-task-id',
                        'Values': [
                            taskid,
                        ]
                    },
                ],
            )
            # print(response6)
            for taskarn in response6['ReplicationTasks']:
                # print(taskarn)
                arn = taskarn['ReplicationTaskArn']
                sourceendpointarn = taskarn['SourceEndpointArn']
                targetendpointarn = taskarn['TargetEndpointArn']
                print(arn)
                print(sourceendpointarn)
                print(targetendpointarn)
                response8 = client2.test_connection(
                    ReplicationInstanceArn= riarn,
                    EndpointArn = sourceendpointarn
                )
                # print(response8)
                # for testresults in response8['Connection']:
                #     print(testresults)
                #     teststatussource = testresults['Status']
                response9 = client2.test_connection(
                    ReplicationInstanceArn= riarn,
                    EndpointArn = targetendpointarn
                )
                time.sleep(60)
                response10 = client2.describe_connections(
                    Filters=[
                        {
                            'Name': 'endpoint-arn',
                            'Values': [
                                sourceendpointarn,
                            ]
                        },
                    ],
                    
                )
                # print(response10)
                for sourceconnection in response10['Connections']:
                    # print(sourceconnection)
                    instancearn1 = sourceconnection['ReplicationInstanceArn']
                    if riarn == instancearn1:
                        print (instancearn1)
                        connectionstatus1 = sourceconnection['Status']
                        print (connectionstatus1)
                response11 = client2.describe_connections(
                    Filters=[
                        {
                            'Name': 'endpoint-arn',
                            'Values': [
                                targetendpointarn,
                            ]
                        },
                    ],
                    
                )
                for targetconnection in response11['Connections']:
                    # print(sourceconnection)
                    instancearn2 = targetconnection['ReplicationInstanceArn']
                    if riarn == instancearn2:
                        print (instancearn2)
                        connectionstatus2 = targetconnection['Status']
                        print (connectionstatus2)
                
                if connectionstatus1 and connectionstatus2 == 'successful':
                    response7 = client2.start_replication_task(
                        ReplicationTaskArn= arn,
                        StartReplicationTaskType='start-replication'
                    )

# response2 = client.describe_stacks(
#     StackName= stackname
#     )
# for metadata in response2['Stacks']:
#     status = metadata['StackStatus']
#     for output in metadata['Outputs']:
#         outputvalue = output['OutputValue']
#         if status == 'CREATE_COMPLETE':
#             print('complete')
#             print(outputvalue)
#             response3 = client2.describe_replication_instances(
#                 Filters=[
#                     {
#                         'Name': 'replication-instance-id',
#                         'Values': [
#                             outputvalue,
#                         ]
#                     },
#                 ],
#             )
#             for ri in response3['ReplicationInstances']:
#                 ristatus = ri['ReplicationInstanceStatus']
#                 riarn = ri['ReplicationInstanceArn']
#                 print(ristatus)
#                 if ristatus == 'available':
#                     response4 = client.create_stack(
#                         StackName = taskstackname,
#                         TemplateURL = 'https://2env-en411-edp-cloudformation-s3.s3.amazonaws.com/dms/ondemand/ri-task-ondemand-cf.yml',
#                         Parameters=[
#                             {
#                                 'ParameterKey': 'ReplicationServerARN',
#                                 'ParameterValue': riarn
                                
#                             },
#                         ], 
#                         Tags=[
#                             {
#                                 'Key': 'AppCode',
#                                 'Value': 'EN411'
#                             },
#                         ],
#                         )
#                     print(response4)
#                     response5 = client.describe_stacks(
#                         StackName= taskstackname
#                         )
#                     for taskmetadata in response5['Stacks']:
#                         for taskdeatils in taskmetadata['Outputs']:
#                             if taskdeatils["OutputKey"] == 'ReplicationTaskName':
#                                 taskid = taskdeatils["OutputValue"]
#                                 print(taskid)
#                                 response6 = client2.describe_replication_tasks(
#                                     Filters=[
#                                         {
#                                             'Name': 'replication-task-id',
#                                             'Values': [
#                                                 taskid,
#                                             ]
#                                         },
#                                     ],
#                                 )
#                                 # print(response6)
#                                 for taskarn in response6['ReplicationTasks']:
#                                     arn = taskarn['ReplicationTaskArn']
#                                     print(arn)
#                                     response7 = client2.start_replication_task(
#                                         ReplicationTaskArn= arn,
#                                         StartReplicationTaskType='start-replication'
#                                     )

