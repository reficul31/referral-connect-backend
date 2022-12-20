import json
import boto3
import base64
import pymysql

from decimal import Decimal

SQS_QUEUE_NAME = 'NewJobEncodingQueue'
JOB_TABLE_NAME = 'job-listings'
JOB_USER_TABLE_NAME = 'user-job-map'

ENDPOINT="referral-connecct.cwmit30emefp.us-east-1.rds.amazonaws.com"
PORT="3306"
USER="admin"
REGION="us-east-1f"
DBNAME="referral_connect"
PASSWORD="masterpassword"
INSERT_QUERY="INSERT INTO JobUserMapping(link,email,company,role,status) values('{}', '{}', '{}', '{}', '{}')"

def lambda_handler(event, context):
    print("Event:{}".format(event))
    email = ''
    if 'secret' not in event['headers']:
        return {
            'isBase64Encoded': False,
            'statusCode': 401,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET'
            },
            'body': 'User secret not found. Please login and try again.'
        }
    
    email = event['headers']['secret']
    print("Asking referrals for user with email: {}".format(email))
    
    """
    Initialize the resources that will be needed in this function
    """
    client = boto3.resource('dynamodb')
    table = client.Table(JOB_TABLE_NAME)
    sqs = boto3.client('sqs')
    
    # Decode the event body since it is base64 encoded
    event = json.loads(base64.b64decode(event["body"]))
    """
    Check whether the job already exists in our database
    """
    response = table.get_item(Key={'link':event["link"]})
    if "Item" in response:
        print("Job already present in job listings database")
        try:
            conn = pymysql.connect(host=ENDPOINT, user=USER, passwd=PASSWORD, db=DBNAME, connect_timeout=5)
            print(INSERT_QUERY.format(event['link'], email, event['company'], event['role'], 'queued'))
            with conn.cursor() as cur:
                cur.execute(INSERT_QUERY.format(event['link'], email, event['company'], event['role'], 'queued'))
            conn.commit()
        except Exception as e:
            print("Error while connecting to the database. {}".format(str(e)))
            return {
                'isBase64Encoded': False,
                'statusCode': 500,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET'
                },
                'body': str(e)
            }
        return {
            'isBase64Encoded': False,
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET'
            },
            'body':'User applied for the job successfully'
        }
    
    print("Adding job to job listings database")
    try:
        event['encoding'] = ''
        response = table.put_item(Item=event)
        print("Table Response: {}".format(response))
    except Exception as e:
        print("Table error: {}".format(e))
        return {
            'isBase64Encoded': False,
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET,POST'
            },
            'body':'Encountered an error while adding job to the table. Error: {}'.format(str(e))
        }
    
    """
    Add the new registered job to the SQS encoding queue to create encodings
    """
    try:
        url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)['QueueUrl']
        response = sqs.send_message(QueueUrl=url, MessageBody=json.dumps(event))
        print("SQS Response: {}".format(response))
    except Exception as e:
        table.delete_item(Key={'link': event['link']})
        print("SQS Error: {}".format(e))
        return {
            'isBase64Encoded': False,
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET'
            },
            'body':'Encountered an error while adding job to encoding queue. Error: {}'.format(str(e))
        }

    try:
        conn = pymysql.connect(host=ENDPOINT, user=USER, passwd=PASSWORD, db=DBNAME, connect_timeout=5)
        print(INSERT_QUERY.format(event['link'], email, event['company'], event['role'], 'scoring'))
        with conn.cursor() as cur:
            cur.execute(INSERT_QUERY.format(event['link'], email, event['company'], event['role'], 'scoring'))
        conn.commit()
    except Exception as e:
        print("Error while connecting to the database. {}".format(str(e)))
        return {
            'isBase64Encoded': False,
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET'
            },
            'body': str(e)
        }
    
    return {
        'isBase64Encoded': False,
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET'
        },
        'body':'Job link added to listings'
    }