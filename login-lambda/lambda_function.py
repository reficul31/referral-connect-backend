import json
import hashlib
import boto3
import base64

USER_DATA_TABLE_NAME = 'user-data'
salt = 'sb4539,ya2467,sg4021'

def lambda_handler(event, context):
    print("Event: {}".format(event))
    
    """
    Initialize the resources that will be needed in this function
    """
    client = boto3.resource('dynamodb')
    table = client.Table(USER_DATA_TABLE_NAME)
    
    # Decode the event body since it is base64 encoded
    event = json.loads(base64.b64decode(event["body"]))
    
    # Hash the password
    password = str(hashlib.md5((event['password']+salt).encode()).hexdigest())
    
    """
    Check whether the user already exist in our database
    """
    response = table.get_item(Key={'userId':event["email"]}, AttributesToGet=['password'])
    if "Item" not in response or password != response['Item']['password']:
        return {
            'isBase64Encoded': False,
            'statusCode': 401,
             'headers': {
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET'
            },
            'body':'User or password is incorrect.'
        }
    
    return {
        'isBase64Encoded': False,
        'statusCode': 200,
         'headers': {
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET'
        },
        'body': event['email']
    }