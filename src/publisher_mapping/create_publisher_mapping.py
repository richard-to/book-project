"""
Maps an official publisher to a bib number in the SPL inventory.

This step will be separate from the main ETL process since I don't want to
set up Elasticsearch on AWS.

Instead we'll do this on a local Elasticsearch instance.
"""
import configparser

from elasticsearch import Elasticsearch
from fuzzywuzzy import process
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read("etl.cfg")

ES_HOST = config.get("publishers", "elasticsearch_host")
ES_INDEX_NAME = "book_publishers"

MATCH_THRESHOLD = 89

SPL_DATA_DICT_PATH = config.get("spl", "data_dict_path")
SPL_INVENTORY_PATH = config.get("spl", "inventory_path")

PUBLISHERS_PATH = "publishers_data"

NUM_PARTITIONS = 6  # Number of partitions to use when processing publishers


def main():
    spark = SparkSession.builder.appName("SPL-Publishers").getOrCreate()

    data_dict_df = spark.read.option("header", "true").csv(SPL_DATA_DICT_PATH)
    all_inventory_df = spark.read.option("header", "true").csv(SPL_INVENTORY_PATH)

    raw_publishers_df = filter_book_publishers(all_inventory_df, data_dict_df)
    publishers_with_official_df = find_official_publishers(spark, raw_publishers_df)

    # Manually upload the output file to S3
    publishers_with_official_df.write.mode("overwrite").csv(PUBLISHERS_PATH)


def filter_book_publishers(all_inventory_df, data_dict_df):
    """Filters inventory to unique publishers

    Args:
        all_inventory_df: SPL inventory data
        data_dict_df: SPL data dictionary

    Returns:
        Unique publishers of print books
    """
    return (
        all_inventory_df
        .join(data_dict_df, data_dict_df.Code == all_inventory_df.ItemType)
        .filter(data_dict_df["Code Type"] == "ItemType")
        .filter(data_dict_df["Format Group"] == "Print")
        .filter(data_dict_df["Format Subgroup"] == "Book")
        .select(all_inventory_df.Publisher.alias("publisher"))
        .distinct()
        .na.drop()
    )


def find_official_publishers(spark, raw_publishers_df):
    """Attempts to find matching publisher in the official list

    Args:
        spark: Connection to spark
        raw_publishers_df: Raw publishers to find matches for

    Returns:
        Dataframe with publishers that have an official match
    """
    matched_publishers_rdd = (
        raw_publishers_df
        .coalesce(1)
        # Since the number of publishers is a reasonable number, we can create a
        # sequentially incrementing ID column by using one partition
        .withColumn("id", F.monotonically_increasing_id())

        # Use an RDD to batch process publishers since it will be more performant to
        # run elasticsearch queries with the same connection.
        .coalesce(NUM_PARTITIONS)
        .rdd
        # Round robin dispatch publishers across the four processes
        .map(lambda r: (r[1] % NUM_PARTITIONS, r[0]))
        .groupByKey()

        .flatMap(query_official_publisher)
    )
    return spark.createDataFrame(matched_publishers_rdd).toDF("publisher", "official_publisher")


def query_official_publisher(data):
    _, publishers = data
    es = Elasticsearch([ES_HOST])
    matched_publishers = []
    for publisher in publishers:
        res = es.search(
            index=ES_INDEX_NAME,
            body={
                "query": {
                    "match": {
                        "publisher": {
                            "query": publisher,
                        },
                    },
                },
            },
            size=10,
        )
        matches = [m["_source"]["publisher"] for m in res["hits"]["hits"]]

        # Elasticsearch will try to return some matches, but they may not be quality
        # matches, so we will use fuzzywuzzy to ensure a minimum threshold
        result = process.extractOne(publisher, matches, score_cutoff=MATCH_THRESHOLD)
        if result:
            matched_publishers.append((publisher, result[0]))
    es.transport.close()

    return matched_publishers


if __name__ == "__main__":
    main()
