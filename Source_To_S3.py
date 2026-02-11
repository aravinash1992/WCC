# pip install boto3
# pip install python-dotenv

import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# 🔑 LOAD .env FILE
load_dotenv()

AWS_REGION = "us-east-1"
BUCKET_NAME = "calibo-dia-tma"
LOCAL_FILE_PATH = r"C:\Users\arajak\Downloads\VOLUME.csv"
S3_KEY = "Source/VOLUME.csv"

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def upload_file_to_s3():
    try:
        s3_client.upload_file(LOCAL_FILE_PATH, BUCKET_NAME, S3_KEY)
        print("✅ File uploaded successfully to S3")
    except FileNotFoundError:
        print("❌ Local file not found")
    except NoCredentialsError:
        print("❌ AWS credentials not found")
    except ClientError as e:
        print(f"❌ AWS error: {e}")

if __name__ == "__main__":
    upload_file_to_s3()
