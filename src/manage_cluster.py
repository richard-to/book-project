import argparse
import configparser
import json

import boto3
from botocore.exceptions import ClientError
import pandas as pd


config = configparser.ConfigParser()
config.read_file(open("etl.cfg"))

AWS_KEY = config.get("aws", "key")
AWS_SECRET = config.get("aws", "secret")

REDSHIFT_CLUSTER_TYPE = config.get("redshift", "cluster_type")
REDSHIFT_NUM_NODES = config.get("redshift", "num_nodes")
REDSHIFT_NODE_TYPE = config.get("redshift", "node_type")

REDSHIFT_CLUSTER_IDENTIFIER = config.get("redshift", "cluster_identifier")
REDSHIFT_DB = config.get("redshift", "db_name")
REDSHIFT_DB_USER = config.get("redshift", "db_user")
REDSHIFT_DB_PASSWORD = config.get("redshift", "db_password")
REDSHIFT_PORT = config.get("redshift", "db_port")

REDSHIFT_IAM_ROLE_NAME = config.get("redshift", "iam_role_name")


ec2 = boto3.resource(
    "ec2",
    region_name="us-west-2",
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
)

iam = boto3.client(
    "iam",
    region_name="us-west-2",
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
)

redshift = boto3.client(
    "redshift",
    region_name="us-west-2",
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
)


def main():
    """Entry point to manage_cluster.py CLI"""

    actions_map = {
        "show_cluster_endpoint_and_role": show_cluster_endpoint_and_role,
        "check_status": check_status,
        "create_cluster": create_cluster,
        "enable_vpc_access": enable_vpc_access,
        "delete_cluster": delete_cluster,
        "delete_role": delete_role,
    }

    parser = argparse.ArgumentParser(description="Manage Redshift Cluster")
    parser.add_argument("action", choices=list(actions_map.keys()))
    args = parser.parse_args()

    actions_map[args.action]()


def create_cluster():
    """Creates a Redshift cluster based on dwh.cfg if it does not exist"""
    try:
        redshift.describe_clusters(ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER)
        print("Redshift cluster already exists")
        return
    except redshift.exceptions.ClusterNotFoundFault:
        print("Creating Redshift cluster...")

    # Create IAM for Redshift cluster
    try:
        iam.create_role(
            Path="/",
            RoleName=REDSHIFT_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps({
                "Statement": [{
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "redshift.amazonaws.com",
                    },
                }],
                "Version": "2012-10-17",
            }),
        )
    except Exception as e:
        print(e)

    iam.attach_role_policy(
        RoleName=REDSHIFT_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )['ResponseMetadata']['HTTPStatusCode']

    redshift_role_arn = iam.get_role(RoleName=REDSHIFT_IAM_ROLE_NAME)["Role"]["Arn"]

    # Create Redshift cluster
    try:
        redshift.create_cluster(
            # Hardware config
            ClusterType=REDSHIFT_CLUSTER_TYPE,
            NodeType=REDSHIFT_NODE_TYPE,
            NumberOfNodes=int(REDSHIFT_NUM_NODES),

            # Identifiers & Credentials
            DBName=REDSHIFT_DB,
            ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
            MasterUsername=REDSHIFT_DB_USER,
            MasterUserPassword=REDSHIFT_DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[redshift_role_arn],
        )
    except Exception as e:
        print(e)


def check_status():
    """Checks the status of the Redshift cluster"""
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER)["Clusters"][0]
    except redshift.exceptions.ClusterNotFoundFault:
        print("Error: Cluster does not exist.")
        return

    print("Checking cluster status...")
    print(pretty_redshift_props(cluster_props))


def enable_vpc_access():
    """Enables VPC access to Redshift cluster

    - The Redshift cluster will be assigned to the default security group
    - The default security does not enable access to the Redshift cluster
    - For educational purposes, we will allow public access to the cluster
    """
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER)["Clusters"][0]
    except redshift.exceptions.ClusterNotFoundFault:
        print("Error: Cluster does not exist.")
        return

    vpc = ec2.Vpc(id=cluster_props['VpcId'])

    default_sg = None
    for security_group in vpc.security_groups.all():
        if security_group.group_name == "default":
            default_sg = security_group
            break
    try:
        print("Enabling VPC access to cluster...")
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(REDSHIFT_PORT),
            ToPort=int(REDSHIFT_PORT),
        )
    except ClientError:
        print("VPC access already enabled.")


def show_cluster_endpoint_and_role():
    """Gets the Redshift cluster endpoint and role ARN"""
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER)["Clusters"][0]
    except redshift.exceptions.ClusterNotFoundFault:
        print("Error: Cluster does not exist.")
        return

    if cluster_props["ClusterStatus"] != "available":
        print(f"Error: Cluster is not available. Current status is: {cluster_props['ClusterStatus']}")
        return

    cluster_endpoint = cluster_props["Endpoint"]["Address"]
    cluster_role_arn = cluster_props["IamRoles"][0]["IamRoleArn"]

    print("Showing cluster endpoint and role ARN...")
    print(f"REDSHIFT_ENDPOINT :: {cluster_endpoint}")
    print(f"REDSHIFT_ROLE_ARN :: {cluster_role_arn}")


def delete_cluster():
    """Deletes the Redshift cluster if it exists"""
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER)["Clusters"][0]
    except redshift.exceptions.ClusterNotFoundFault:
        print("Error: Cluster does not exist.")
        return

    if cluster_props["ClusterStatus"] != "available":
        print(f"Error: Cluster is not available. Current status is: {cluster_props['ClusterStatus']}")
        return

    print("Deleting cluster...")
    redshift.delete_cluster(ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)


def delete_role():
    """Deletes the Redshift cluster IAM role"""
    print("Deleting role...")
    iam.detach_role_policy(
        RoleName=REDSHIFT_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )
    iam.delete_role(RoleName=REDSHIFT_IAM_ROLE_NAME)


def pretty_redshift_props(props):
    """Prints Redshift cluster props"""
    pd.set_option("display.max_colwidth", -1)
    keys_to_show = [
        "ClusterIdentifier",
        "NodeType",
        "ClusterStatus",
        "MasterUsername",
        "DBName",
        "Endpoint",
        "NumberOfNodes",
        'VpcId',
    ]
    cluster_data = [(k, v) for k, v in props.items() if k in keys_to_show]
    return pd.DataFrame(data=cluster_data, columns=["Key", "Value"])


if __name__ == "__main__":
    main()
