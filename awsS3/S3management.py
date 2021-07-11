#!/usr/bin/python3

"""
Purpose:
    Script to use in basic operations on AWS service S3.
"""

from support.credentials import Access_Key_ID, Secret_Access_Key, Region
from botocore.exceptions import ClientError
from datetime import datetime
import logging
import boto3
import os


class S3transfer:

    def __init__(self):
        """
        self.Access_Key_ID: Access Key Id AWS account user
        self.Secret_Access_Key: Secret Access Key AWS account user
        self.Region: Region Bucket
        """

        try:
            self.Access_Key_ID = Access_Key_ID
            self.Secret_Access_Key = Secret_Access_Key
            self.Region = Region

            # create session with AWS credentials
            self.session = boto3.session.Session(aws_access_key_id=self.Access_Key_ID,
                                                aws_secret_access_key=self.Secret_Access_Key,
                                                region_name=self.Region)
            # create client AWS S3 service
            self.s3_client = self.session.resource(service_name='s3')

        except Exception as e:
            print('Client dont work. Check credentials or Network connection.')

    def return_client(self):
        return self.s3_client

    def s3_upload(self, file_input, bucket, object_name=None):
        """
        Upload function for files in S3 buckets
        :param file_input: files will be upload in bucket
        :param bucket: S3 Bucket name
        :param object_name: filename on S3
        """
        # Verify object name
        if object_name is None:
            object_name = file_input

        try:
            # script main to use upload object in S3.
            self.s3_client.Object(bucket_name=bucket, key=object_name).upload_file(Filename=file_input)

        except ClientError as e:
            logging.error(e)
            return False
        return True

    def s3_download(self, bucket, object_name, directory):
        """
        Download function for files S3 buckets for local servers
        :param bucket: S3 Bucket name
        :param object_name: filename on S3
        :param directory: directory name on local server
        """
        try:

            self.s3_client.Object(bucket, object_name).download_file(os.path.join(directory, object_name))
            
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def s3_delete(self, bucket, object_name):
        """
        Delete function for files in S3 buckets
        :param bucket: S3 Bucket name
        :param object_name: filename on S3
        """
        try:
            self.s3_client.Object(bucket, object_name).delete()        
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def list_buckets(self):
        """
        List active buckets in AWS at active session
        """

        response = self.s3_client.buckets.all()
        buckets = list()

        for buck in response:
            buckets.append(buck.name)

        return buckets

    def list_object(self, bucket_name):
        """
        List objects inside S3 Buckets
        :param bucket_name: S3 Bucket name
        """
        bucket = self.s3_client.Bucket(bucket_name)

        object_list = list()

        for obj in bucket.objects.all():
            object_list.append(obj.key)

        return object_list

    def put_bucket_parquet(self, bucket_name, filename, parquet_buffer):
        """
        put object format parquet in S3 Buckets 
        :param bucket_name: S3 Bucket name
        :param filename: object name 
        :param parquet_buffer: binary parquet file in memory allocation
        """
        try:            
            self.s3_client.Object(bucket_name, filename + '.parquet').put(Body=parquet_buffer.getvalue())             

        except Exception as e:
            print(str(datetime.now()) + ' -> Erro no processo de carga do arquivo ' + filename)    
            print('Erro apresentado ' + str(e)) 
     