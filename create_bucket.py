import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='password123'
)

s3.create_bucket(Bucket='cicd-logs')

print("Bucket created successfully!")