import boto3

def lambda_handler(event, context):
    
    conn = boto3.client("emr")       
    cluster_id = conn.run_job_flow(
        Name='LearningCluster',
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        VisibleToAllUsers=True,
        LogUri='s3n://project-logs-n/elasticmapreduce-learning/',
        ReleaseLabel='emr-5.8.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm3.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Slave nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm3.xlarge',
                    'InstanceCount': 10,
                }
            ],
            'Ec2KeyName': 'project',
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': True
        },
        Applications=[{
            'Name': 'Spark'
        }],
        BootstrapActions=[{
            'Name': 'Install BOTO3',
            'ScriptBootstrapAction': {
                'Path': 's3://project-test-n/code/ml-bootstrap.sh'
            }
        }],
        Steps=[{
            'Name': 'Cleaning Data',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                   "/usr/bin/spark-submit",
                   "--deploy-mode",
                   "cluster",
                   "s3://project-test-n/code/complete.py"
                ]
            }
        }]
    )
    
        
    

    return cluster_id