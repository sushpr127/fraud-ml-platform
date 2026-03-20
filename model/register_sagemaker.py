import boto3
import pickle
import os
import tarfile
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv('S3_BUCKET')
REGION    = os.getenv('AWS_REGION', 'us-east-1')

s3 = boto3.client('s3',
    region_name           = REGION,
    aws_access_key_id     = os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY'),
)

sagemaker = boto3.client('sagemaker',
    region_name           = REGION,
    aws_access_key_id     = os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY'),
)

print("Packaging model artifact...")
with tarfile.open('models/model.tar.gz', 'w:gz') as tar:
    tar.add('models/fraud_xgb.pkl',    arcname='fraud_xgb.pkl')
    tar.add('models/feature_meta.pkl', arcname='feature_meta.pkl')

print("Uploading model to S3...")
s3.upload_file(
    'models/model.tar.gz',
    S3_BUCKET,
    'models/fraud_xgb/model.tar.gz',
)
model_s3_uri = f"s3://{S3_BUCKET}/models/fraud_xgb/model.tar.gz"
print(f"Model uploaded to: {model_s3_uri}")

print("Creating SageMaker Model Package Group...")
try:
    sagemaker.create_model_package_group(
        ModelPackageGroupName        = 'fraud-xgboost',
        ModelPackageGroupDescription = 'XGBoost fraud detection model versions',
    )
    print("Model package group created: fraud-xgboost")
except sagemaker.exceptions.ClientError as e:
    if 'already exists' in str(e).lower():
        print("Model package group already exists — skipping")
    else:
        raise

with open('models/feature_meta.pkl', 'rb') as f:
    meta = pickle.load(f)

print("Registering model version in SageMaker...")
response = sagemaker.create_model_package(
    ModelPackageGroupName       = 'fraud-xgboost',
    ModelPackageDescription     = f"XGBoost fraud classifier AUC {meta['auc']:.4f}",
    ModelApprovalStatus         = 'Approved',
    CustomerMetadataProperties  = {
        'auc':              str(round(meta['auc'], 4)),
        'n_estimators':     str(meta['n_estimators']),
        'top_feature':      'ProductCD',
    },
    InferenceSpecification = {
        'Containers': [{
            'Image':        '683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.7-1',
            'ModelDataUrl': model_s3_uri,
        }],
        'SupportedContentTypes':                  ['text/csv'],
        'SupportedResponseMIMETypes':             ['text/csv'],
        'SupportedTransformInstanceTypes':        ['ml.m5.large'],
        'SupportedRealtimeInferenceInstanceTypes':['ml.m5.large'],
    },
)
print(f"Model registered successfully")
print(f"Model Package ARN: {response['ModelPackageArn']}")
print(f"AUC: {meta['auc']:.4f}")
