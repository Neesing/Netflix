import boto3

def lambda_handler(event, context):
    
    conn = boto3.client("emr")       
    cluster_id = conn.run_job_flow(
        Name='CleaningCluster',
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        VisibleToAllUsers=True,
        LogUri='s3n://project-test-n/elasticmapreduce/',
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
                    'InstanceCount': 1,
                }
            ],
            'Ec2KeyName': 'project',
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': True
        },
        Applications=[{
            'Name': 'Spark'
        }],
        Configurations=[{
            "Classification":"spark-env",
            "Properties":{},
            "Configurations":[{
                "Classification":"export",
                "Properties":{
                    "PYSPARK_PYTHON":"python35",
                    "PYSPARK_DRIVER_PYTHON":"python35"
                }
            }]
        }],
         BootstrapActions=[{
            'Name': 'Install BOTO3',
            'ScriptBootstrapAction': {
                'Path': 's3://project-test-n/code/bootstrap.sh'
            }
        }],
        Steps=[{
            'Name': 'Cleaning Data',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3n://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': [
                   's3://project-test-n/code/python_script.sh'
                ]
            }
        }]
    )
    
        
    

    return cluster_id