import json
import boto3
import pymysql

ENDPOINT="referral-connecct.cwmit30emefp.us-east-1.rds.amazonaws.com"
PORT="3306"
USER="admin"
REGION="us-east-1f"
DBNAME="referral_connect"
PASSWORD="masterpassword"

JOB_KEYS=['role', 'company', 'link', 'status']
SELECT_DATA_STRING = "SELECT role,company,link,status from JobUserMapping where email='{}'"
CREATE_TABLE_STRING = """CREATE TABLE IF NOT EXISTS JobUserMapping (
link varchar(255) not null,
email varchar(255) not null,
company varchar(255) not null,
role varchar(255) not null,
score DECIMAL(2, 2) default null,
status set('scoring', 'queued', 'matched') default 'scoring',
primary key (link, email))
"""

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
    print("Getting referrals for user with email: {}".format(email))
    try:
        conn = pymysql.connect(host=ENDPOINT, user=USER, passwd=PASSWORD, db=DBNAME, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_STRING)
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
    
    referrals = []
    try:
        with conn.cursor() as cur:
            cur.execute(SELECT_DATA_STRING.format(email))
            for row in cur:
                referrals.append({key:row[i] for i, key in enumerate(JOB_KEYS)})
    except Exception as e:
        print("Error while querying the database. {}".format(str(e)))
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
        'body': json.dumps(referrals)
    }