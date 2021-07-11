
credentials_s3_aws = \
open(r'_s3_accessKeys.csv', 'r').readlines()[
    1].split(',')


Access_Key_ID = credentials_s3_aws[0]
Secret_Access_Key = credentials_s3_aws[1]
Region = credentials_s3_aws[2]

