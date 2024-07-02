import boto3
import os
import spacy
import zipfile
import glob
import json
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('files')  



def download_and_unzip_model(bucket_name, s3_key, local_path):
    s3 = boto3.client('s3')
    zip_path = '/tmp/model.zip'
    s3.download_file(bucket_name, s3_key, zip_path)
    print(f"Downloaded zip file from S3: {zip_path}")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(local_path)
    print(f"Extracted zip file to: {local_path}")

def pos_tagging(name, description):
    processed_text = spacy_pipeline(description)
    tagged_text = " ".join([token.pos_ for token in processed_text])
    return name.upper() + " || " + tagged_text

def process_rows(rows):
    return [{'name': row['name'], 'description': row['description'], 
             'nlp_output': pos_tagging(row['name'], row['description'])} 
            for row in rows]
def load_model():
    bucket_name = 'naftali-files'
    s3_key = 'model.zip'
    local_model_path = '/tmp'
    
    download_and_unzip_model(bucket_name, s3_key, local_model_path)
    
    # Try to find the correct model path
    possible_model_paths = glob.glob('/tmp/en_core_web_sm-3.0.0')
    if possible_model_paths:
        model_path = possible_model_paths[0]
        print(f"Found model path: {model_path}")
    else:
        print("Could not find the model directory. Here's what's in /tmp:")
        
        raise Exception("Could not find the model directory")

    global spacy_pipeline
    try:
        spacy_pipeline = spacy.load(model_path, disable=['parser', 'lemmatizer'])
        print("Successfully loaded spaCy model")
    except Exception as e:
        print(f"Error loading spaCy model: {str(e)}")
        raise
    
load_model()

def lambda_handler(event, context):
    # Check if the body is present in the event
    if 'body' not in event:
        return {
            'statusCode': 400,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*"
             },
            'body': json.dumps('No body found in the request')
        }

    # Parse the body content
    try:
        body = json.loads(event['body'])
    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*"
             },
            'body': json.dumps('Invalid JSON in request body')
        }

    # Check if rows are present in the body
    if 'rows' not in body:
        return {
            'statusCode': 400,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*"
             },
            'body': json.dumps('Missing rows in request body')
        }

    rows = body['rows']

    # Extract userId from event.requestContext.identity
    try:
        user_id = event['requestContext']['identity']['cognitoIdentityId']
    except KeyError:
        return {
            'statusCode': 400,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*"
             },
            'body': json.dumps('Unable to extract userId from request context')
        }

    try:
        updated_rows = process_rows(rows)
        
        # Save updated_rows to DynamoDB
        table.put_item(
            Item={
                'userId': user_id,
                'processedRows': updated_rows
            }
        )

        # Return only the status code
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*"
             }
        }
    except Exception as e:
        print(f"Error processing rows: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*"
             },
            'body': json.dumps(f'Error processing rows: {str(e)}')
        }
