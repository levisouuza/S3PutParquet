#!/usr/bin/python3

from awsS3.S3management import S3transfer
from datetime import datetime
import pandas as pd 
import io
import re

bucket_name = 'bucket-name'

S3 = S3transfer()
client = S3.return_client()
bucket = client.Bucket(bucket_name)

start_process = datetime.now()

for obj in bucket.objects.all():

    if re.search('.csv', obj.key):
    
        print('\n' + str(datetime.now()) + ' -> Arquivo a ser transformado: ' + obj.key)
        filename = obj.key.split('.', 1)[0]

        start = datetime.now()

        try:
            print(str(datetime.now()) + ' -> Lendo arquivo')
            df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))
            parquet_buffer = io.BytesIO()

            print(str(datetime.now()) + ' -> Transformando em parquet')
            df.to_parquet(parquet_buffer, index=False) 

            print(str(datetime.now()) + ' -> Carregando no Bucket')       
            S3.put_bucket_parquet(bucket_name, filename, parquet_buffer)
                        
            print(str(datetime.now()) + ' -> Arquivo ' + filename + '.parquet Carregado no Bucket') 

            print(str(datetime.now()) + ' -> Deletando arquivo ' + obj.key) 
            S3.s3_delete(bucket_name, obj.key)

            finish = datetime.now()

            print(str(datetime.now()) + ' -> Tempo de processo arquivo: ' + str(finish - start))

        except Exception as e:
            print(str(datetime.now()) + ' -> Erro no processo de carga do arquivo ' + filename)    
            print('Erro apresentado ' + str(e)) 
                    

finish_process = datetime.now()

print('\n' + str(datetime.now()) + ' -> Tempo Total do processo de carga: ' + str(finish_process - start_process))