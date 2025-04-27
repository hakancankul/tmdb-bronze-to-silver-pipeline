import findspark
findspark.init("/opt/spark")

import boto3
from botocore.config import Config

# MinIO bağlantısı
s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='dataops',
    aws_secret_access_key='Ankara06',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

bucket_names = ["tmdb-bronze", "tmdb-silver"]

for bucket_name in bucket_names:
    try:    
        response = s3.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            for obj in response["Contents"]:
                s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
            print(f"{bucket_name} içeriği temizlendi.")
        else:
            print(f"{bucket_name} zaten boş.")

    except s3.exceptions.NoSuchBucket:        
        s3.create_bucket(Bucket=bucket_name)
        print(f"{bucket_name} oluşturuldu.")
    
    except Exception as e:
        print("Hata:", e)