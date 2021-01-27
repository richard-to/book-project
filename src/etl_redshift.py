"""
Loads star schema data in S3 into Redshift.
"""
import configparser

import boto3
import psycopg2


def main():
    """Entrypoint to drop/create Redshift tables"""

    # Load config
    config = configparser.ConfigParser()
    config.read("etl.cfg")

    aws_key = config.get("aws", "key")
    aws_secret = config.get("aws", "secret")

    db_cluster_id = config.get("redshift", "cluster_identifier")
    db_name = config.get("redshift", "db_name")
    db_user = config.get("redshift", "db_user")
    db_password = config.get("redshift", "db_password")
    db_port = config.get("redshift", "db_port")

    redshift = boto3.client(
        "redshift",
        region_name="us-west-2",
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
    )

    # Make sure the Redshift cluster exists
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=db_cluster_id)["Clusters"][0]
    except redshift.exceptions.ClusterNotFoundFault:
        print("Error: Cluster does not exist.")
        return

    if cluster_props["ClusterStatus"] != "available":
        print(f"Error: Cluster is not available. Current status is: {cluster_props['ClusterStatus']}")
        return

    # Dynamically retrieve the Redshift cluster host
    db_host = cluster_props["Endpoint"]["Address"]

    # Connect to Redshift cluster
    conn = psycopg2.connect(
        f"host={db_host} dbname={db_name} user={db_user} password={db_password} port={db_port}"
    )

    # Dynamically retrieve Role ARN so we can access S3 buckets
    role_arn = cluster_props["IamRoles"][0]["IamRoleArn"]

    # Drop tables before recreating them to ensure a clean environment
    with conn.cursor() as cursor:
        tables = [
            "dim_book",
            "dim_author",
            "dim_subject",
            "dim_publisher",
            "dim_checkout_time",
            "br_book_author",
            "br_book_subject",
            "fact_spl_book_checkout",
        ]
        for table in tables:
            print(f"Loading data for {table}...", end=" ")
            query = """
            COPY {table}
            FROM '{s3_path}'
            IAM_ROLE '{role_arn}'
            FORMAT AS PARQUET
            """.format(
                table=table,
                s3_path=config.get("output", f"{table}_path"),
                role_arn=role_arn,
            )
            cursor.execute(query)
            print("OK")
            conn.commit()

    conn.close()


if __name__ == "__main__":
    main()
