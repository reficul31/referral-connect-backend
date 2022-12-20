import json
import boto3

EXTRACTED_USER_KEYS = ['userId', 'education', 'experience']
SQS_QUEUE_NAME = 'NewUserEncodingQueue'
USER_DATA_TABLE_NAME = 'user-data'
NLU_SEARCH_MODEL_ENDPOINT = 'nlu-search-model-1671477168'

def lambda_handler(event, context):
    """
    Initialize the resources that will be needed in this function
    """
    sqs = boto3.client('sqs')
    client = boto3.resource('dynamodb')
    table = client.Table(USER_DATA_TABLE_NAME)
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
    Extract the users from SQS queue for which encodings are needed
    """
    users = []
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
                'body': 'No users present in the queue'
            }
        
        for message in response['Messages']:
            body = json.loads(message['Body'])
            user = {key: body[key] for key in EXTRACTED_USER_KEYS}
            user['ReceiptHandle'] = message['ReceiptHandle']
            users.append(user)
    except Exception as e:
        print("SQS Error: {}".format(str(e)))
        return {
            'statusCode': 500,
            'body': 'Error during querying SQS. Error: {}'.format(str(e))
        }
    
    print("Extracted Users: {}".format(users))
    
    """
    Create encodings for the user
    """
    try:
        for user in users:
            encoding = {}
            for index, experience in enumerate(user['experience']):
                payload = ' '.join([experience['company'], experience['role'], experience['description']])
                response = runtime.invoke_endpoint(EndpointName=NLU_SEARCH_MODEL_ENDPOINT, ContentType='text/plain', Body=payload)
                encoding["experience_{}".format(index)] = str(response['Body'].read())
            print('User encodings:', encoding)
            user['encoding'] = json.dumps(encoding)
    except Exception as e:
        print("Sagemaker Error: {}".format(str(e)))
        return {
            'statusCode': 500,
            'body': str(e)
        }
    
    """
    Update user encodings in DB
    """
    try:
        for user in users:
            response = table.update_item(
                Key={'userId': user['userId']},
                UpdateExpression="SET #E = :e",
                ExpressionAttributeNames={
                    '#E': 'encoding'
                },
                ExpressionAttributeValues={
                    ':e': user['encoding']
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
        for user in users:
            response = sqs.delete_message(
                QueueUrl=url,
                ReceiptHandle=user['ReceiptHandle']
            )
            print("SQS Delete Response: {}".format(response))
    except Exception as e:
        print("SQS Error: {}".format(str(e)))
        return {
            'statusCode': 500,
            'body': 'Error during deleting from SQS queue. Error: {}'.format(str(e))
        }

    return {
        'statusCode': 200,
        'body':'Successfully created user encodings and updated in DB'
    }