import json
import boto3
import uuid
from datetime import datetime,timedelta
import random

# Initialize the SQS client
sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/905418357916/AirbnbBookingQueue'


def generate_anb_booking_data():
    return {
        "bookingId": str(uuid.uuid4()),
        "userId": str(uuid.uuid4()),
        "propertyId": str(uuid.uuid4()),
        "location": f"{random.choice(['New York', 'Berlin', 'Tokyo'])}, {random.choice(['USA', 'Germany', 'Japan'])}",
        "startDate": (datetime.now() + timedelta(days=random.randint(1, 10))).strftime('%Y-%m-%d'),
        "endDate": (datetime.now() + timedelta(days=random.randint(11, 20))).strftime('%Y-%m-%d'),
        "price": f"{random.randint(100, 500)} USD"
    }

def lambda_handler(event, context):
    bookings = []  # Use a list to collect all bookings
     
    for _ in range(10):  # Generate and send 10 booking messages
        anb_bkg = generate_anb_booking_data()
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(anb_bkg)
        )
        bookings.append(anb_bkg)  # Append each booking to the list

    return {
        'statusCode': 200,
        'body': json.dumps('Message sent to SQS queue successfully.')
    }

