import json
import boto3
import csv
from io import StringIO

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('files') 

def lambda_handler(event, context):
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

    http_method = event['httpMethod']

    if http_method == 'GET':
        return get_data(user_id)
    elif http_method == 'PUT':
        return put_data(user_id, event)
    else:
        return {
            'statusCode': 405,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*"
            },
            'body': json.dumps('Method not allowed')
        }

def get_data(user_id):
    try:
        response = table.get_item(Key={'userId': user_id})
        
        if 'Item' not in response:
            return {
                'statusCode': 200,
                'headers': {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "*"
                },
                'body': json.dumps('No data found for this user')
            }
        processed_rows = response['Item']['processedRows']
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Content-Type": "application/json"
            },
            'body': json.dumps(processed_rows)
        }
    except Exception as e:
        print(f"Error retrieving data: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*"
            },
            'body': json.dumps(f'Error retrieving data: {str(e)}')
        }

def put_data(user_id, event):
    try:
        body = json.loads(event['body'])
        csv_data = body['csvData']
        
        csv_file = StringIO(csv_data)
        csv_reader = csv.DictReader(csv_file)
        
        processed_rows = []
        for row in csv_reader:
            processed_rows.append({
                'name': row.get('Name', ''),
                'description': row.get('Description', ''),
                'nlp_output': row.get('NLP Output', '')
            })

        table.put_item(
            Item={
                'userId': user_id,
                'processedRows': processed_rows
            }
        )

        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*"
            },
            'body': json.dumps('Data saved successfully')
        }
    except Exception as e:
        print(f"Error saving data: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*"
            },
            'body': json.dumps(f'Error saving data: {str(e)}')
        }
