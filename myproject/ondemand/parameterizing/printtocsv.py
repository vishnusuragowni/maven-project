import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import numpy as np
import os
import boto3
client = boto3.client('ec2')
THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
my_file = os.path.join(THIS_FOLDER, 'EC2-Qradar.xlsx')

df = pd.read_excel(my_file, sheet_name='Sheet2')

list1 = df['EC2NAMES']
list2 = df['EC2InstanceType']

for names in list1:
    print(names)
for instancetype in list2:
    print(instancetype)
#     instances = client.describe_instances(Filters=custom_filter)
#     for instance in instances['Reservations']:
#         for key in instance["Instances"]:
#             x = key['InstanceId']
#             print(x)
#             # this is the best aspproach for creating dictionary in pandas
#             data.append([names, x])
# pd.DataFrame(data, columns=['A','B']).to_csv('df111112.csv')