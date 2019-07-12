import boto3

AMI_SERVER = "ami-009c174642dba28e4"  #ami ubuntu server
KEY_PEM = "XX"

AWS_ACCESS_KEY_ID = "XXXXX"
AWS_SECRET_ACCESS_KEY = "XXXX"

INSTANCE_TYPE_MASTER = "m4.xlarge"
INSTANCE_TYPE_CORE = "m4.xlarge"

NUM_CORES_CLUSTER = 2

REGION = "eu-central-1"
AVAILABILITY_ZONE = "c"



#############################################  START SCRIPT ################################################



#########  START EMR ############
clientEmr = boto3.client(
    'emr',
    region_name=REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


clientEmr.run_job_flow(
    Name='clusterFlink',
    ReleaseLabel='emr-5.24.1',
    Instances={
        'InstanceGroups': [
            {
                'Name': 'instanceGroupMaster',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': INSTANCE_TYPE_MASTER,
                'InstanceCount': 1
            },
            {
                'Name': 'instanceGroupCore',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': INSTANCE_TYPE_CORE,
                'InstanceCount': NUM_CORES_CLUSTER
            }
        ],
        'Ec2SubnetId': 'subnted ID XXX',
        'Ec2KeyName': KEY_PEM,
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': True,
    },
    # Define applications to install in the cluster
    Applications=[
        {
            'Name': 'Hadoop'
        },
        {
            'Name': 'Flink'
        }
    ],
    Configurations=[
        {
            "Classification": "hdfs-site",
            "Properties": {
                   "dfs.permissions.enabled": "false"
             }
        },
    ],
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole'
)

print ("Cluster started")
