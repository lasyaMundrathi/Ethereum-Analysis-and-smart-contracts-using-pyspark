import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":
    
    spark = SparkSession\
    .builder\
    .appName("etherum")\
    .getOrCreate()


    def good_transaction_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            int(fields[11])
            int(fields[9])
            return True
        except:
            return False
    
    
    def mapper(line):
        try:
            fields = line.split(',')
            raw_timestamp = int(fields[11])#timestamp
            key = time.strftime('%Y-%m W%W', time.gmtime(raw_timestamp))

            gas_price = int(fields[9])#gasprice
            return (key, (gas_price, 1))
        except:
            return ('dummy', (0, 1))
    
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    #transactions
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + 
                                               "/ECS765/ethereum-parvulus/transactions.csv") 
    clean_transactions = transactions.filter(good_transaction_line)
    trans = clean_transactions.map(mapper)
    trans_step2 = trans.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
    trans_step3 = trans_step2.mapValues(lambda x: int(round(x[0]/x[1]/1000000000)))
    step4 = trans_step3.sortByKey()
    print('Week, Gas Price in Gwei')
    for row in step4.collect():
        print('{}, {}'.format(row[0], row[1]))

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'gasguzzelers_' + date_time + '/transactions.txt')
    my_result_object.put(Body=json.dumps(step4.take(200)))
    spark.stop()
    
