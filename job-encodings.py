import json
import boto3
import pymysql

ENDPOINT="referral-connecct.cwmit30emefp.us-east-1.rds.amazonaws.com"
PORT="3306"
USER="admin"
REGION="us-east-1f"
DBNAME="referral_connect"
PASSWORD="masterpassword"
UPDATE_TABLE_QUERY="UPDATE JobUserMapping SET status='matching' WHERE link='{}'"

EXTRACTED_JOB_KEYS = ['link', 'description', 'preferredQualifications', 'requiredQualifications']
SQS_QUEUE_NAME = 'NewJobEncodingQueue'
JOB_DATA_TABLE_NAME = 'job-listings'
NLU_SEARCH_MODEL_ENDPOINT = 'nlu-search-model-1671477168'

def lambda_handler(event, context):
    """
    Initialize the resources that will be needed in this function
    """
    sqs = boto3.client('sqs')
    client = boto3.resource('dynamodb')
    table = client.Table(JOB_DATA_TABLE_NAME)
    runtime = boto3.Session().client('sagemaker-runtime')
    
    """
    Get SQS Queue URL
    """
    url = ''
    try:
        url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)['QueueUrl']
    except Exception as e:
        print("Queue URL error: {}".format(str(e)))
        return {
            'statusCode': 500,
            'body': str(e)
        }
    
    """
    Extract the jobs from SQS queue for which encodings are needed
    """
    jobs = []
    try:
        response = sqs.receive_message(
            QueueUrl=url,
            AttributeNames=['All'],
            MaxNumberOfMessages=10,
            VisibilityTimeout=1
        )
        print("SQS Response: {}".format(response))
        
        if 'Messages' not in response:
            return {
                'statusCode': 200,
                'body': 'No jobs present in the queue'
            }
        
        for message in response['Messages']:
            body = json.loads(message['Body'])
            job = {key: body[key] for key in EXTRACTED_JOB_KEYS}
            job['ReceiptHandle'] = message['ReceiptHandle']
            jobs.append(job)
    except Exception as e:
        print("SQS Error: {}".format(str(e)))
        return {
            'statusCode': 500,
            'body': 'Error during querying SQS. Error: {}'.format(str(e))
        }
    
    print("Extracted Jobs: {}".format(jobs))
    
    """
    Create encodings for the job
    """
    try:
        for job in jobs:
            response = runtime.invoke_endpoint(EndpointName=NLU_SEARCH_MODEL_ENDPOINT, ContentType='text/plain', Body=job['description'])
            print('Sagemaker Response: {}'.format(response))
            job['encoding'] = str(response['Body'].read())
            print('Encoding: {}'.format(job['encoding']))
    except Exception as e:
        return {
            'statusCode': 500,
            'body': 'Error while calling the sagemaker endpoint.' + str(e)
        }
    
    """
    Update job encodings in DB
    """
    try:
        for job in jobs:
            response = table.update_item(
                Key={'link': job['link']},
                UpdateExpression="SET #E = :e",
                ExpressionAttributeNames={
                    '#E': 'encoding'
                },
                ExpressionAttributeValues={
                    ':e': job['encoding']
                }
            )
            print("DynamoDB response: {}".format(response))
    except Exception as e:
        print("DynamoDB Error: {}".format(str(e)))
        return {
            'statusCode': 500,
            'body': 'Error during querying DynamoDB update. Error: {}'.format(str(e))
        }
    
    """
    Clear messages from queue
    """
    try:
        for job in jobs:
            response = sqs.delete_message(
                QueueUrl=url,
                ReceiptHandle=job['ReceiptHandle']
            )
            print("SQS Delete Response: {}".format(response))
    except Exception as e:
        print("SQS Error: {}".format(str(e)))
        return {
            'statusCode': 500,
            'body': 'Error during deleting from SQS queue. Error: {}'.format(str(e))
        }
    
    try:
        conn = pymysql.connect(host=ENDPOINT, user=USER, passwd=PASSWORD, db=DBNAME, connect_timeout=5)
        with conn.cursor() as cur:
            for job in jobs:
                cur.execute(UPDATE_TABLE_QUERY.format(job['link']))
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
        'statusCode': 200,
        'body':'Successfully created job encodings and updated in DB'
    }