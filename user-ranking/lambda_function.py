import json
import boto3
import requests
import pymysql
import numpy as np

from numpy import dot
from numpy.linalg import norm
from datetime import datetime

ENDPOINT="referral-connecct.cwmit30emefp.us-east-1.rds.amazonaws.com"
PORT="3306"
USER="admin"
REGION="us-east-1f"
DBNAME="referral_connect"
PASSWORD="masterpassword"
QUERY_TABLE_QUERY="SELECT email, link from JobUserMapping where status='queued' and score is null LIMIT 1"
UPDATE_TABLE_QUERY="UPDATE JobUserMapping SET score={} where email='{}' and link='{}'"

USER_DATA_TABLE_NAME = 'user-data'
JOB_DATA_TABLE_NAME = 'job-listings'

NUM_SECONDS_YEARS = 60*60*24*30*12
NLU_SEARCH_MODEL_ENDPOINT = 'nlu-search-model-1671477168'
QA_SEARCH_MODEL_ENDPOINT = "qa-model-1671486736"
BASIC_QUESTIONS = {
    'education_level': 'what education is required?',
    'degree_major': 'what fields of study are required?',
    'yoe': 'how many years of experience is required?'
}

def lambda_handler(event, context):
    job_id, user_id = "", ""
    try:
        conn = pymysql.connect(host=ENDPOINT, user=USER, passwd=PASSWORD, db=DBNAME, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute(QUERY_TABLE_QUERY)
            for row in cur:
                user_id = row[0]
                job_id = row[1]
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
    
    if len(job_id) == 0 or len(user_id) == 0:
        return {
            'statusCode': 200,
            'body': 'No jobs in pipeline.'
        }
    
    try:
        basic_score = compute_job_details_similarity(job_id, user_id)
    except Exception as e:
        basic_score = 0
        print(e)
    # print("basic score: ", compute_job_details_similarity(job_id, user_id))
    
    try:
        user_embeds = get_user_features(user_id)
        job_embeds = get_job_features(job_id)
        
        nlu_score = sentence_transformers_score(user_embeds, job_embeds)
    except Exception as e:
        nlu_score = 0
        print(e)
    # print("nlu score: ", nlu_score)
    
    score = basic_score*10 + nlu_score*100
    try:
        conn = pymysql.connect(host=ENDPOINT, user=USER, passwd=PASSWORD, db=DBNAME, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute(UPDATE_TABLE_QUERY.format(score, user_id, job_id))
        conn.commit()
    except Exception as e:
        print("Error while updating the database. {}".format(str(e)))
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
        'body': {
            'email': user_id,
            'link': job_id,
            'score': score
        }
    }
    # print(get_user_features('trijeet.sethi@gmail.com'))
    # print('user ed: ', get_user_education('trijeet.sethi@gmail.com'))
    
    # compute_job_details_similarity(1)
    # compute_embeddings_from_str('hi what is your name')
    # # get user embeddings = {'encoding_0': np(array), 'encoding_1':..}
    
    
    # data = query_qa_model(
    #     'MS or PhD in Computer Science, Statistics, Mathematics, or equivalent. 1-3 years (with PhD) or 3-5 years (with MS) of industrial experience in a related field. Industrial experience with one or more of the following: classification, regression, recommendation systems, targeting systems, ranking systems, fraud detection, online advertising, or related. Experience in big data processing, e.g. Hadoop, SQL, Spark. Experience with Python or R, and Java or Scala or C/C++. 2 or more related publications in quality conferences or journals.'
    #     )
        
    # # # print(sentence_transformers_score(1, 1))
    
    # return {
    #     'statusCode': 200,
    #     'body': data
    # }
    

def compute_total_experience_time(user_id):
    client = boto3.resource('dynamodb')
    table = client.Table(USER_DATA_TABLE_NAME)
    
    res = table.get_item(
        Key = {
            'userId': user_id
        }
    )
    
    d = res['Item']['experience']
    total_exp_seconds = 0.0
    try:
        for exp in d:
            d_to = datetime.strptime(exp['dateTo'], "%Y-%m-%d")
            d_from = datetime.strptime(exp['dateFrom'], "%Y-%m-%d")
            d_res = d_to - d_from
            total_exp_seconds += (d_res.total_seconds()/NUM_SECONDS_YEARS)
    except Exception as e:
        print(e)
        

    return total_exp_seconds
    
def get_user_education(user_id):
    client = boto3.resource('dynamodb')
    table = client.Table(USER_DATA_TABLE_NAME)
    
    res = table.get_item(
        Key = {
            'userId': user_id
        }
    )
    edu_str = ''
    d = res['Item']['education']
    
    for edu in d:
        edu_str = edu_str + edu['description'] + edu['level'] + '. '
    
    return edu_str
    
def get_user_features(user_id):
    client = boto3.resource('dynamodb')
    table = client.Table(USER_DATA_TABLE_NAME)
    
    res = table.get_item(
        Key = {
            'userId': user_id
        }
    )
    
    resp = json.loads(res['Item']['encoding'])
    result = {}
    for k in resp.keys():
        temp = resp[k][3:-2].split(', ')
        result[k] = np.array([float(x) for x in temp])
        
    return result
    
def get_job_features(job_id):
    job_id = "https://www.amazon.jobs/en/jobs/2173488/software-development-engineer-intech"
    client = boto3.resource('dynamodb')
    table = client.Table(JOB_DATA_TABLE_NAME)
    
    res = table.get_item(
        Key = {
            'link': job_id
        }
    )
    
    resp = (res['Item']['encoding'])
    
    temp = resp[3:-2].split(', ')
    result = np.array([float(x) for x in temp])
        
    return result
    
def query_qa_model(qualifications):
    runtime = boto3.Session().client('sagemaker-runtime')
    job_details = {}
    # qualifications = job['basic_qualifications']
    for k in BASIC_QUESTIONS.keys():
        data_prep = {
        "inputs": {
            "question": BASIC_QUESTIONS[k],
            "context": qualifications
            }
        }
        
        try:
            response = runtime.invoke_endpoint(
            EndpointName=QA_SEARCH_MODEL_ENDPOINT, 
            ContentType='application/json', 
            Body=json.dumps(data_prep).encode('utf-8')
            )
        except Exception as e:
            job_details[k] = ''
            # return {
            #     'statusCode': 500,
            #     'body': 'Error while calling the sagemaker endpoint.' + str(e)
            # }
        # print(json.loads(response['Body'].read())['answer'])
        job_details[k] = json.loads(response['Body'].read())['answer']
    
    return job_details

def compute_job_details_similarity(job_id, user_id):
    # say for now
    job_id = "https://www.amazon.jobs/en/jobs/2173488/software-development-engineer-intech"
    client = boto3.resource('dynamodb')
    table = client.Table(JOB_DATA_TABLE_NAME)
    
    res = table.get_item(
        Key = {
            'link': job_id
        }
    )
    
    user_embeds = get_user_features(user_id)
    user_edu = get_user_education(user_id)
    user_edu_embed = compute_embeddings_from_str(user_edu)
    user_total_exp = compute_total_experience_time(user_id)
    user_total_exp_str = "I have " + str(int(user_total_exp)) + " years of experience."
    user_total_exp_embed = compute_embeddings_from_str(user_total_exp_str)
    
    score = []
    for qual in ['requiredQualifications', 'preferredQualifications']:
        qual_text = res['Item'][qual]
        qual_details_dict = query_qa_model(qual_text)
        qual_text_embed = compute_embeddings_from_str(qual_text)
        
        try:
            job_req_edu = qual_details_dict['education_level'] + 'in ' + qual_details_dict['degree_major']
            job_req_edu_embed = compute_embeddings_from_str(job_req_edu)
            score.append(cosine_helper(job_req_edu_embed, user_edu_embed))
        except Exception as e:
            print(e)
            score.append(0)
        
        try:
            job_req_yoe = qual_details_dict['yoe'] + " years of experience is required."
            job_req_yoe_embed = compute_embeddings_from_str(job_req_yoe)
            score.append(cosine_helper(job_req_yoe_embed, user_edu_embed))
        except Exception as e:
            print(e)
            score.append(0)
        
        try:
            score.append(sentence_transformers_score(user_embeds, qual_text_embed))
        except Exception as e:
            score.append(0)
    
    score = np.array(score)
    final_score = (np.mean(score[0:3]) + 1.5*np.mean(score[3:]))/2
    return final_score
   
def cosine_helper(a, b):
    return dot(a, b)/(norm(a)*norm(b))

def compute_embeddings_from_str(text_str):
    runtime = boto3.Session().client('sagemaker-runtime')
    try:
        res = runtime.invoke_endpoint(
            EndpointName=NLU_SEARCH_MODEL_ENDPOINT, 
            ContentType='text/plain', 
            Body=text_str
            )
    except Exception as e:
        return {
            'statusCode': 500,
            'body': 'Error while calling the sagemaker endpoint.' + str(e)
        }
        # print(e)
        
    # print(res['Body'].read())
    response_str = res['Body'].read().decode('utf-8')
    resp_list = json.loads(response_str)
    # print("score:", sentence_transformers_score(np.array(json.loads(response_str)), np.array(json.loads(response_str))))
    return np.array(resp_list)

def sentence_transformers_score(user_embeds, job_embeds):
    u_embeddings = list(user_embeds.values())
    j_embedding = job_embeds
    
    # # remove later:
    # u_embeddings = [[1, 2, 3], [2, 2, 2]] # list of len -> no. of experiences
    # j_embedding = [1, 1, 1] # list of 1 embedding 
    
    scores = []
    for u_e in u_embeddings:
        a, b = u_e, j_embedding
        cosine_similarity = dot(a, b)/(norm(a)*norm(b))
        scores.append(cosine_similarity)
    
    scores = np.array(scores)
    max_val = np.amax(scores)
    mean_val = np.mean(scores)
    
    score = round(((max_val + mean_val)/2), 5)
    
    return score