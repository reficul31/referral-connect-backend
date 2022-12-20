import json
import hashlib
import boto3
import base64

SQS_QUEUE_NAME = 'NewUserEncodingQueue'
USER_DATA_TABLE_NAME = 'user-data'
salt = 'sb4539,ya2467,sg4021'

def lambda_handler(event, context):
    print("Event: {}".format(event))
    
    """
    Initialize the resources that will be needed in this function
    """
    client = boto3.resource('dynamodb')
    table = client.Table(USER_DATA_TABLE_NAME)
    sqs = boto3.client('sqs')
    
    # Decode the event body since it is base64 encoded
    event = json.loads(base64.b64decode(event["body"]))
    
    """
    Check whether the user already exist in our database
    """
    checkIfUserAlreadyRegistered = table.get_item(Key={'userId':event["personalInformation"]["email"]})
    if "Item" in checkIfUserAlreadyRegistered:
        return {
            'isBase64Encoded': False,
            'statusCode': 409,
             'headers': {
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET'
            },
            'body':'User already registered'
        }
    
    """
    Prepare the raw data
    """
    userPassword = str(hashlib.md5((event["personalInformation"]['password']+salt).encode()).hexdigest())
    event['password'] = userPassword
    event['encoding'] = ''
    event['userId'] = event["personalInformation"]["email"]
    del event["personalInformation"]["password"]
    
    """
    Insert into DynamoDB table
    """
    try:
        response = table.put_item(Item=event)
        print("Table Response: {}".format(response))
    except Exception as e:
        print("Table Error: {}".format(e))
        return {
            'isBase64Encoded': False,
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET'
            },
            'body':'Encountered an error while adding user to the table. Error: {}'.format(str(e))
        }
    
    """
    Add the new registered user to the SQS encoding queue to create encodings
    """
    try:
        url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)['QueueUrl']
        response = sqs.send_message(QueueUrl=url, MessageBody=json.dumps(event))
        print("SQS Response: {}".format(response))
    except Exception as e:
        table.delete_item(Key={'userId': event['userId']})
        print("SQS Error: {}".format(e))
        return {
            'isBase64Encoded': False,
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET'
            },
            'body':'Encountered an error while adding user to encoding queue. Error: {}'.format(str(e))
        }
    
    return {
        'isBase64Encoded': False,
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET'
        },
        'body':'User registered'
    }