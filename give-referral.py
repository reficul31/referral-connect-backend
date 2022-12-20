import json
import boto3
import base64
import pymysql

USER_DATA_TABLE_NAME='user-data'
JOB_USER_KEYS = ['email','role','score','link']
SELECT_REFERRALS_QUERY="SELECT email,role,score,link from JobUserMapping where status='queued' and company='{}' and email<>'{}'"
UPDATE_DATA_QUERY="UPDATE JobUserMapping SET status='matched' where email='{}' and link='{}'"

ENDPOINT="referral-connecct.cwmit30emefp.us-east-1.rds.amazonaws.com"
PORT="3306"
USER="admin"
REGION="us-east-1f"
DBNAME="referral_connect"
PASSWORD="masterpassword"

def lambda_handler(event, context):
    print("Event: {}".format(event))
    
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
    print("Giving referrals for user with email: {}".format(email))
    
    if event['httpMethod'] == 'POST':
        # Decode the event body since it is base64 encoded
        event = json.loads(base64.b64decode(event["body"]))
        print("Referral: {}".format(event))
        try:
            conn = pymysql.connect(host=ENDPOINT, user=USER, passwd=PASSWORD, db=DBNAME, connect_timeout=5)
            with conn.cursor() as cur:
                cur.execute(UPDATE_DATA_QUERY.format(event['email'], event['link']))
            conn.commit()
        except Exception as e:
            print("Error while connecting/creating the database. {}".format(str(e)))
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
            'body': 'Thank you for referring ' + event['name']
        }
        
    
    """
    Initialize the resources that will be needed in this function
    """
    client = boto3.resource('dynamodb')
    table = client.Table(USER_DATA_TABLE_NAME)
    
    user_company = ''
    try:
        response = table.get_item(Key={'userId': email}, AttributesToGet=['personalInformation'])
        print("User Query: {}".format(response))
        if 'Item' not in response:
            return {
                'isBase64Encoded': False,
                'statusCode': 500,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET'
                },
                'body': 'User not found in the database'
            }
        user_company = response['Item']['personalInformation']['company']
    except Exception as e:
        print("Error getting the user data. {}".format(str(e)))
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

    try:
        conn = pymysql.connect(host=ENDPOINT, user=USER, passwd=PASSWORD, db=DBNAME, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute(SELECT_REFERRALS_QUERY.format(user_company, email))
    except Exception as e:
        print("Error while connecting/creating the database. {}".format(str(e)))
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
    
    referrals = []
    for row in cur:
        response = table.get_item(Key={'userId': row[0]}, AttributesToGet=['personalInformation', 'resume'])
        if 'Item' in response:
            referral = {key: row[i] for i, key in enumerate(JOB_USER_KEYS)}
            referral['name'] = response['Item']['personalInformation']['firstName'] + ' ' + response['Item']['personalInformation']['lastName']
            referral['resume'] = response['Item']['resume']
            referral['score'] = str(referral['score'])
            referrals.append(referral)
    
    print("Referrals: {}".format(referrals))
    return {
        'isBase64Encoded': False,
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,PUT,GET'
        },
        'body': json.dumps(referrals)
    }
