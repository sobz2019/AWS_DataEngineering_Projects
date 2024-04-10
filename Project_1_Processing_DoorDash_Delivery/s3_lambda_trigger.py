import json
import pandas as pd
import boto3
import requests
import datetime

s3 = boto3.client('s3')
def lambda_handler(event, context):
    # Get the bucket name and object key from the event
    print(event)
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    
    today_date = datetime.datetime.now().strftime("%Y-%m-%d")
    object_key = f"{today_date}-raw_input.json"

    
    # Read the JSON file from S3
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    
    json_data = response['Body'].read().decode('utf-8')
    data = json.loads(json_data)

    
    # Filter records with 'processed' status
    processed_records = [record for record in data if record.get('status') == 'processed']
    print('hi')
    
    # Create a Pandas DataFrame
    df = pd.DataFrame(processed_records)
    
    # Convert DataFrame to CSV
    csv_data = df.to_csv(index=False)
    
    targt_bucket_name='doordash-target-zonegds'

    # Upload CSV to S3
    csv_key = object_key.split('.')[0] + '.csv'  # Assuming same file name but with .csv extension
    s3.put_object(Bucket=targt_bucket_name, Key=csv_key, Body=csv_data)
    
    return {
        'statusCode': 200,
        'body': json.dumps('CSV file created and uploaded successfully!')
    }
    
    
