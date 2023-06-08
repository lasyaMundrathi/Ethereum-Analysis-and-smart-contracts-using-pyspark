import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
#Evaluate the top 10 smart contracts by total Ether received. You will need to join address field in the contracts dataset to the to_address in the transactions dataset to determine how much ether a contract has received.
if __name__ == "__main__":
    
    spark = SparkSession\
    .builder\
    .appName("ethereum")\
    .getOrCreate()

    def good_contract_line(line):
        try:
            fields = line.split(',')
            if len(fields)<=4:
                return False
            return True
        except:
            return False

    def good_transaction_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            int(fields[7])
            return True
        except:
            return False

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
    
    #contracts
    contracts = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + 
                                            "/ECS765/ethereum-parvulus/contracts.csv")
    clean_contracts = contracts.filter(good_contract_line)
    contract_address = clean_contracts.map(lambda l: (l.split(',')[0],'contract'))#address
 
    #transactions
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + 
                                               "/ECS765/ethereum-parvulus/transactions.csv") 
    clean_transactions = transactions.filter(good_transaction_line)
    transaction_to_address = clean_transactions.map(lambda l: (l.split(',')[6],int(l.split(',')[7]))) #to_address,value
    transactions=transaction_to_address.reduceByKey(lambda x,y: x+y)

    #joining and finding the top 10 most popular address 
    contract_data = transactions.join(contract_address)
    
    top10 = contract_data.takeOrdered(10, key=lambda x: -x[1][0])
    print(top10)
    
    print('===========')
    for record in top10:
        print("{},{}".format(record[0], int(record[1][0]/1000000000000000000)))
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum11_' + date_time + '/top10.txt')
    my_result_object.put(Body=json.dumps(top10))

    spark.stop()
    