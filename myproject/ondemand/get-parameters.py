import boto3

# s3 = boto3.client('s3')
# bucket='2env-en411-edp-cloudformation-s3'
# result = s3.list_objects(Bucket = bucket, Prefix='/dms/ondemand/parameters.txt')
# for o in result.get('Contents'):
#     data = s3.get_object(Bucket=bucket, Key=o.get('Key'))
#     contents = data['Body'].read()
#     print(contents)
s3 = boto3.client('s3')
data = s3.get_object(Bucket='2env-en411-edp-cloudformation-s3', Key='dms/ondemand/parameters.json')
contents = data['Body'].read().decode('utf-8')
ritype = contents
print(ritype)
