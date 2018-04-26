#!/bin/sh
  
aws s3 cp s3://project-test-n/code/cleaning.py /tmp/cleaning.py

python /tmp/cleaning.py