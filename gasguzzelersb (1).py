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
    
    def good_contract_line(line):
        try:
            fields = line.split(',')
            if len(fields)<=4:
                return False
            return True
        except:
            return False
        
    def mapper(line):
        try:
            fields = line.split(',')
            raw_timestamp = int(fields[11])
            year_month = time.strftime('%Y-%m W%W', time.gmtime(raw_timestamp))
            key = (fields[6], year_month)

            #print('Timestamp {0} to month {1}'.format(raw_timestamp, key))
            gas_supplied = int(fields[8])
            return (key, (gas_supplied, 1))
        except:
            pass

    def shift_key(x):
        try:
            # key = (to_addr, week number), value = (gas, count)
            # key = to_addr, value = (week_number, gas, count)
            return (x[0][0], (x[0][1], x[1][0], x[1][1]))
        except:
            pass

    def read_contract(line):
        try:
            fields = line.split(',')
            return (fields[0], 'Contract')
        except:
            pass

    def shift_key_contract(x):
        try:
            # key = to_addr, value = ((week_number, gas, count), 'Contract' or None)
            # key = ('Contract' or 'Wallet', week_number), value = (gas, count)
            addr_type = 'Wallet' if x[1][1] is None else x[1][1]
            week_num = x[1][0][0]
            total_gas = x[1][0][1]
            total_trxn = x[1][0][2]
            return ((addr_type, week_num), (total_gas, total_trxn))
        except:
            pass

    def shift_key_results(x):
        try:
            return (x[0][1], (x[0][0], x[1]))
        except:
            pass
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
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv") 
    contracts = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    clean_contracts = contracts.filter(good_contract_line)
    clean_transactions = transactions.filter(good_transaction_line)
    print(clean_transactions.take(1))
    print(clean_contracts.take(2))
    
    transactions = clean_transactions.map(mapper)\
                    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))\
                    .map(shift_key)\
                    .leftOuterJoin(clean_contracts.map(read_contract))\
                    .map(shift_key_contract)\
                    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))\
                    .mapValues(lambda x: x[0]/x[1])\
                    .map(shift_key_results)\
                    .sortByKey()
                    
    for row in transactions.collect():#YearMonthWeek,Year,WeekNum,Type,AverageGas'
        print('{0},{1},{2},{3},{4}'.format(row[0], row[0][0:4], row[0][9:], row[1][0], row[1][1]))

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'gasguzzelersb_' + date_time + '/gasguzzelersb.txt')
    my_result_object.put(Body=json.dumps(step9.take(1000)))
    spark.stop()
    