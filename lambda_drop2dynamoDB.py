import boto3
import json
import csv
import codecs
from urllib.parse import unquote_plus

s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')

# Advice you to add your own try-catch error
def write_to_dynamo(rows):
    table = dynamodb.Table('medium-snowflake2dynamodb-demo')
    
    with table.batch_writer() as batch:
        for i in range(len(rows)):
            batch.put_item(Item=rows[i])
            
def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    obj = s3.Object(bucket, key).get()['Body']
    
    batch_size = 100
    batch = []
 
    # DictReader is a generator type object, does not store objects into memory, but only usable once
    for row in csv.DictReader(codecs.getreader('utf-8')(obj)):
        if len(batch) >= batch_size:
            write_to_dynamo(batch)
            batch.clear()
        batch.append(row)
    
    if batch:
        write_to_dynamo(batch)
        
    return {
        'statusCode': 200,
        'body': json.dumps('Uploaded to DynamoDB!')
    }
