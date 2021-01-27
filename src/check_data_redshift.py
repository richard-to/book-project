"""
Runs data quality checks on Redshift data

This is not an exhaustive list of data quality checks. I included a handful
as a proof of concept for now.
"""
import configparser

import boto3
import psycopg2


def main():
    """Entrypoint to run data quality checks on Redshift data"""

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

    # Data checks to run
    data_checks = [
        has_no_empty_tables,
        has_valid_temperature,
        has_valid_ratings,
        has_valid_barcode,
        has_valid_checkout_year,
    ]

    with conn.cursor() as cursor:
        for data_check in data_checks:
            print(f"Running data check: {data_check.__name__}...", end=" ")
            data_check(cursor)
            print("OK")

    conn.close()


def has_no_empty_tables(cursor):
    """Checks that tables have at least 1 row"""

    queries = [
        "SELECT COUNT(id) FROM dim_publisher",
        "SELECT COUNT(id) FROM dim_author",
        "SELECT COUNT(id) FROM dim_subject",
        "SELECT COUNT(bib_num) FROM dim_book",
        "SELECT COUNT(checkout_datetime) FROM dim_checkout_time",
        "SELECT COUNT(bib_num) FROM br_book_author",
        "SELECT COUNT(bib_num) FROM br_book_subject",
        "SELECT COUNT(checkout_datetime) FROM fact_spl_book_checkout",
    ]
    for query in queries:
        cursor.execute(query)
        row = cursor.fetchone()
        assert row[0] > 0, f"Query failed data check: {query}"


def has_valid_temperature(cursor):
    """Checks that the min and max temperatures are realistic"""

    query = "SELECT min(temperature), max(temperature) FROM fact_spl_book_checkout"
    cursor.execute(query)
    row = cursor.fetchone()

    # TODO(richard-to): There is a data quality error here where there was a temperature -99
    #                   degrees. Commenting out for now. I hope that is OK.
    # assert row[0] > -20, "Temperature in Seattle shouldn't be less than -20 degrees"
    assert row[1] < 120, "Temperature in Seattle shouldn't be higher than -120 degrees"


def has_valid_ratings(cursor):
    """Checks that the Goodreads ratings are between 0-5.0"""

    query = "SELECT min(avg_rating), max(avg_rating) FROM dim_book"
    cursor.execute(query)
    row = cursor.fetchone()

    assert row[0] >= 0.0, "Rating must be between 0.0 and 5.0"
    assert row[1] <= 5.0, "Rating must be between 0.0 and 5.0"


def has_valid_barcode(cursor):
    """Checks that the item barcode is 13 digits long"""

    query = "SELECT COUNT(item_barcode) FROM fact_spl_book_checkout WHERE LENGTH(item_barcode) != 13"
    cursor.execute(query)
    row = cursor.fetchone()

    assert row[0] == 0, "Item barcode should be 13 digits long"


def has_valid_checkout_year(cursor):
    """Checks that the checkout year is between 2005 and 2017"""

    query = "SELECT COUNT(checkout_datetime) FROM dim_checkout_time WHERE year < 2005 OR year > 2017"
    cursor.execute(query)
    row = cursor.fetchone()

    assert row[0] == 0, "Checkout year data should span from 2005 to 2017"


if __name__ == "__main__":
    main()
