# How to run the pipeline

## 0. Install python dependencies

Note: I have python 3.7.4 installed via Anaconda on my local machine.

In the `src` directory run:

```
pip install -r requirements.txt
```

## 1. Config file

In order to run `etl_spark.py` and `etl_redshift.py`, the `etl.cfg` file must be configured:

- **AWS section**
  - An AWS access key and secret will be needed for reading/writing output to your private S3 bucket
- **Goodreads section**
  - Path to Goodreads data in S3
  - Source data here: https://www.kaggle.com/jealousleopard/goodreadsbooks
- **Output section**
  - Output paths to each table in the schema
- **Publishers section**
- Path to generated CSV with map of SPL publishers and Wikipedia publishers list
- See src/publisher_mapping for generating this file
- **Redshift section**
  - Some defaults are specified in the example config, so you'll just need to specify a password
- **SPL section**
  - Paths to SPL checkout data in S3
  - Source data here: https://www.kaggle.com/seattle-public-library/seattle-library-checkout-records
- **Weather section**
  - Path to Seattle weather data in S3
  - Source data here: https://academic.udayton.edu/kissock/http/Weather/gsod95-current/WASEATTL.txt

## 2. Running etl_s3.py on EMR

### 2.1 Create a Key Pair

In order to connect to your cluster, we'll need an EC2 key pair. You can create one via the [AWS console](https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#KeyPairs:).

You'll need to specify the key in the next step.

### 2.2 Create EMR cluster

Next create a cluster. You can do this manually via the [AWS console](https://us-west-2.console.aws.amazon.com/elasticmapreduce/home?region=us-west-2#cluster-list).

Or you can use the AWS CLI (note: replace `<YOUR-KEY-NAME>` with your key):

```bash
aws emr create-default-roles

aws emr create-cluster --name spark-data-lake --use-default-roles --release-label emr-5.28.0 --instance-count 3 --applications Name=Spark  --ec2-attributes KeyName=<YOUR-KEY-NAME> --instance-type m5.xlarge --instance-count 3
```

If using the CLI, you can check when your cluster is ready (i.e. has a status of "WAITING") by running this command. You need to
specify the cluster ID which you can retrieve from the output of the `create-cluster` command.

```bash
aws emr describe-cluster --cluster-id <CLUSTER-ID>
```

This previous command will also return a public domain name, which you can use to SSH into your cluster. Look for a line called
`MasterPublicDnsName`. It should have a value that looks like this `ec2-100-20-156-158.us-west-2.compute.amazonaws.com`.

### 2.3 Connect to EMR cluster

- To connect to your cluster, you can use SSH. On Linux/MacOS, you'll need to download your key pair as a PEM file.
- The public domain of your master node can be retrieved from the previous section or the AWS EMR console.
- One last thing you'll need to do in order to connect to your cluster is allow SSH access from your local machine to
  your cluster.
  - This is done by updating the ingress rules of the Security Group for your cluster's master node. You'll want to add
    an inbound rule allowing SSH from the IP of your local machine.

```bash
ssh -i <PEM-KEY-PAIR> hadoop@<CLUSTER-HOST>
```

### 2.4 Use Python3 on EMR cluster

If you created your cluster through the CLI, you'll want to ensure that `spark-submit` runs using Python3 (and not Python2)
since `etl.py` uses Python3 specific syntax. You can do this by updating the `PYSPARK_PYTHON` environment variable.

```bash
export PYSPARK_PYTHON=python3
```

### 2.5 Upload Spark scripts and config

You can use scp to transfer the following files to your cluster.

- etl.cfg
- etl_spark.py

### 2.6 Running etl.py

You should now be able to run `etl_spark.py` on the cluster:

```bash
spark-submit etl_spark.py
```

### 2.7 Delete EMR cluster

```bash
aws emr terminate-clusters --cluster-ids <CLUSTER-ID>
```

## 3. Running etl_redshift.py on Redshift

Run the following commands in the `src` directory. This will load the parquet data tables generated
in the previous step into Redshift.

### 3.1 Create Redshift cluster

Run `python manage_redshift_cluster.py create_cluster`

### 3.2 Wait until the cluster status becomes "available"

Use `python manage_redshift_cluster.py check_status` to check the status of the cluster

### 3.3 Enable VPC access to the cluster

Run `python manage_redshift_cluster.py enable_vpc_access`

- For educational purposes, this will add an ingress rule that will allow open access to the cluster
  - In a production environment, you'd want to manage access more granularly
- The ingress rule will be added to the security group named "default"
  - This is the default security group when creating a Redshift cluster`

### 3.4 Create tables

Run `python create_redshift_tables.py`

### 3.5 Run ETL to load data from S3 into Redshift

Run `python etl_redshift.py`

### 3.6 Run data quality checks

Run `python check_data_redshift.py`

### 3.7 Clean up

Once you're down running queries on the data, you can delete the cluster using this command.

Run `python manage_redshift_cluster.py delete_cluster`
