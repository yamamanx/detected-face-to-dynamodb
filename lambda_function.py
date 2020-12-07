import boto3
from decimal import Decimal
from urllib.parse import unquote_plus


def add_face_details(payload, face_details):
    print(payload)
    attributes = [
        'Smile',
        'Eyeglasses',
        'Sunglasses',
        'Gender',
        'Beard',
        'Mustache',
        'EyesOpen',
        'MouthOpen'
    ]
    for attribute in attributes:
        if attribute in face_details:
            payload[attribute] = Decimal(face_details[attribute]['Confidence'])
    return payload


def add_face_emotions(payload, emotions):
    for emotion in emotions:
        payload[emotion['Type']] = Decimal(emotion['Confidence'])
    return payload


def lambda_handler(event, context):
    rekognition = boto3.client('rekognition')
    table = boto3.resource('dynamodb').Table('ProfilePhotos')
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        face_response = rekognition.detect_faces(
            Image={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': key
                }
            },
            Attributes=[
                'ALL',
            ]
        )
        payload = {
            'Prefix': key.split('/')[0],
            'ObjectKey': key
        }
        payload = add_face_details(
            payload,
            face_response['FaceDetails'][0]
        )
        if 'Emotions' in face_response['FaceDetails'][0]:
            payload = add_face_emotions(
                payload,
                face_response['FaceDetails'][0]['Emotions']
            )

        table.put_item(
            Item=payload
        )

    return {
        'statusCode': 200
    }
