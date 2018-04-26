#!/bin/sh

sudo pip install boto3

aws s3 cp s3://project-test-n/test-data/netflix-test.txt /tmp/netflix-test.txt