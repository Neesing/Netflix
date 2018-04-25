#!/usr/bin/env python3

import boto3
import time
import csv

s3 = boto3.resource('s3')

s3_client = boto3.client('s3')
filepath = "/tmp/hello.txt"
csvpath = "/tmp/helloutput.csv"

# change when move to original account
bucketName= 'project-test-n'

movie_id = 0

bucket = s3.Bucket(bucketName)

outputfile = str("cleaned/output-rating") + str(".csv")
csvpath = str("/tmp/helloutput") + str(".csv")
csvfile = open(csvpath, 'w')
writer = csv.writer(csvfile)

count = 1
for obj in bucket.objects.all():
    print(obj.key)
    if 'combined_data' in obj.key:
        
        print("input" , str(obj.key))
        inputfile = s3_client.download_file(bucketName, obj.key, filepath)
        writer.writerow(["movie_id","customer_id","rating"])

        with open(filepath, 'r') as f:
            for row in f:
                if ":" in row:
                    global movie_id
                    movie_id = row[: -2]
                else:
                    writer.writerow([movie_id, row.split(',')[0], row.split(',')[1]])

        f.close()
        time.sleep(5)
csvfile.close()        
s3.meta.client.upload_file(csvpath, bucketName, outputfile)