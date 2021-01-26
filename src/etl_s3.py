"""
Performs ETL on raw SPL checkout data and create star schema in S3 in parquet format.
"""
import configparser
import os
import re
from typing import Tuple

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType, StructField, StructType
from pyspark.sql.dataframe import DataFrame


GOODREADS_DATE_FORMAT = "M/d/yyyy"
SPL_CHECKOUT_DATETIME_FORMAT = "MM/dd/yyyy h:m:s a"
WEATHER_DATA_SEP = "         "


def main():
    """ETL script

    Processes raw data into star schema for querying SPL checkouts.

    Data sources:
      - SPL checkouts data
      - Goodreads data
      - Temperature data
      - Publisher list data

    Fact:
        - fact_book_checkout

    Dimensions:
        - dim_subject
        - br_book_subject
        - dim_author
        - br_book_author
        - dim_publisher
        - dim_book
        - checkout_datetime
    """
    config = configparser.ConfigParser()
    config.read("etl.cfg")

    # Store AWS access key and secret as environment variables so we can access private S3 buckets
    os.environ["AWS_ACCESS_KEY_ID"] = config.get("aws", "key")
    os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("aws", "secret")

    spark = SparkSession.builder.appName("SPL-Checkouts").getOrCreate()

    try:
        limit_records = config.getint("main", "limit_records")

        data_dict_df = spark.read.option("header", "true").csv(config.get("spl", "data_dict_path"))
        inventory_df = load_inventory_data(spark, data_dict_df, config.get("spl", "inventory_path"), limit_records)
        goodreads_df = load_goodreads_data(spark, config.get("goodreads", "data_path"))
        books_df = link_goodreads_data(inventory_df, goodreads_df)

        # Dimension tables
        create_dim_subjects(
            books_df,
            config.get("output", "dim_subject_path"),
            config.get("output", "br_book_subject_path"),
        )

        create_dim_authors(
            books_df,
            config.get("output", "dim_author_path"),
            config.get("output", "br_book_author_path"),
        )

        publishers_map_df = load_publishers_data(spark, config.get("publishers", "data_path"))
        bib_num_publisher_lookup_df, publishers_df = create_dim_publishers(
            books_df,
            publishers_map_df,
            config.get("output", "dim_publisher_path"),
        )

        create_dim_books(books_df, config.get("output", "dim_book_path"))

        checkouts_df = load_checkouts_data(spark, data_dict_df, config.get("spl", "checkouts_path"), limit_records)
        create_dim_checkout_datetimes(checkouts_df, config.get("output", "dim_checkout_time_path"))

        # Fact table
        weather_df = load_weather_data(spark, config.get("weather", "data_path"))
        create_fact_spl_book_checkouts(
            checkouts_df,
            weather_df,
            bib_num_publisher_lookup_df,
            publishers_df,
            config.get("output", "fact_spl_book_checkout_path"),
        )
    finally:
        spark.stop()


def load_publishers_data(spark: SparkSession, data_path: str) -> DataFrame:
    """Loads publishers map data into spark dataframe

    This data contains a map to a raw publisher to an official publisher. This mapping
    was created in a separate adhoc step. See src/publisher_mapping

    Args:
        spark: Spark session
        publishers_map_path: Path to publishers map

    Returns:
        - Publishers map dataframe
        - Columns:
          - publisher - Raw publisher that can be found in SPL inventory
          - official_publisher - Publisher from Wikipedia list
    """
    df = spark.read.csv(data_path)
    return df.select(
        df._c0.alias("publisher"),
        df._c1.alias("official_publisher"),
    )


def load_weather_data(spark: SparkSession, data_path: str) -> DataFrame:
    """Loads weather data into spark dataframe

    Args:
        spark: Spark session
        weather_path: Path to weather data

    Returns:
        Weather data
    """
    # The weather data delimits columns by spaces, but there can be a different
    # amount of spaces between each column.
    #
    # WEATHER_DATA_SEP separator is the minimum number of spaces to split the columns.
    #
    # Afterwards we'll need to remove the trailing whitespace and cast the data types
    df = spark.read.option("sep", WEATHER_DATA_SEP).csv(data_path)
    return df.select(
        F.trim(df._c0).cast(IntegerType()).alias("month"),
        F.trim(df._c1).cast(IntegerType()).alias("day"),
        F.trim(df._c2).cast(IntegerType()).alias("year"),
        F.trim(df._c3).cast(FloatType()).alias("temperature"),
    )


def load_goodreads_data(spark: SparkSession, data_path: str) -> DataFrame:
    """Loads Goodreads CSV data into a spark dataframe

    Args:
        spark: Spark session
        data_path: Path to Goodreads data

    Returns:
        Goodreads dataframe
    """
    book_schema = StructType([
        StructField("bookID", StringType(), True),
        StructField("title", StringType(), True),
        StructField("authors", StringType(), True),
        StructField("average_rating", FloatType(), True),
        StructField("isbn", StringType(), True),
        StructField("isbn13", StringType(), True),
        StructField("language_code", StringType(), True),
        StructField("  num_pages", IntegerType(), True),  # CSV has extra padding on this column
        StructField("ratings_count", IntegerType(), True),
        StructField("text_reviews_count", IntegerType(), True),
        StructField("publication_date", StringType(), True),
        StructField("publisher", StringType(), True),
    ])
    df = spark.read.schema(book_schema).option("header", "true").csv(data_path)
    return (
        df
        # Convert publication date to a datetime type
        .withColumn(
            "publication_date",
            F.to_timestamp(df.publication_date, GOODREADS_DATE_FORMAT),
        )
        # Fix formatting
        .withColumnRenamed("  num_pages", "num_pages")
    )


def load_inventory_data(
    spark: SparkSession,
    data_dict_df: DataFrame,
    data_path: str,
    limit_records: int = 0,
) -> DataFrame:
    """Loads SPL inventory filtered to print books into spark dataframe

    Args:
        spark: Spark session
        data_dict_df: Used for filtering on print books
        data_path: Path to SPL inventory data
        limit_records: Work with a subset of records. Use 0 for all records.

    Returns:
        - SPL inventory filtered by print books
        - Columns:
          - bib_num
          - isbns
          - raw_title
          - raw_author
          - raw_publication_year
          - raw_publisher
          - raw_subjects
    """
    df = spark.read.option("header", "true").csv(data_path)

    if limit_records:
        df = df.limit(limit_records)

    return (
        df
        .join(data_dict_df, data_dict_df.Code == df.ItemType)
        .filter(data_dict_df["Code Type"] == "ItemType")
        .filter(data_dict_df["Format Group"] == "Print")
        .filter(data_dict_df["Format Subgroup"] == "Book")
        # Multiple bib numbers can appear due to variations in ItemType, ItemCollection, and especially
        # ItemLocation.
        #
        # The checkouts data does not include ItemLocation, so it would not be possible to distinguish
        # each bib number.
        #
        # Instead we'll drop the duplicates, since we don't care about the location of the item. We only
        # care that it's the same book
        .drop_duplicates(["BibNum"])
        .select(
            df.BibNum.alias("bib_num"),
            df.ISBN.alias("isbns"),
            df.Title.alias("raw_title"),
            df.Author.alias("raw_author"),
            df.PublicationYear.alias("raw_publication_year"),
            df.Publisher.alias("raw_publisher"),
            df.Subjects.alias("raw_subjects"),
        )
    )


def load_checkouts_data(
    spark: SparkSession,
    data_dict_df: DataFrame,
    data_path: str,
    limit_records: int = 0,
) -> DataFrame:
    """Loads SPL checkouts filtered to print books into spark dataframe

    Args:
        spark: Spark session
        data_dict_df: Used for filtering on print books
        data_path: Path to SPL checkouts data
        limit_records: Work with a subset of records. Use 0 for all records.

    Returns:
        - SPL checkouts filtered by print books
        - Columns: bib_num, item_barcode, checkout_datetime
    """
    df = spark.read.option("header", "true").csv(data_path)

    if limit_records:
        df = df.limit(limit_records)

    return (
        df
        .join(data_dict_df, data_dict_df.Code == df.ItemType)
        .filter(data_dict_df["Code Type"] == "ItemType")
        .filter(data_dict_df["Format Group"] == "Print")
        .filter(data_dict_df["Format Subgroup"] == "Book")
        .select(
            df.BibNumber.alias("bib_num"),
            df.ItemBarcode.alias("item_barcode"),
            # Convert checkout date time string into a timestamp
            F.to_timestamp(df.CheckoutDateTime, SPL_CHECKOUT_DATETIME_FORMAT).alias("checkout_datetime"),
        )
    )


def link_goodreads_data(inventory_df: DataFrame, goodreads_df: DataFrame) -> DataFrame:
    """Links goodreads data with SPL books inventory

    We attempt to match Goodreads data with the SPL inventory if possible. The
    Goodreads data is not only a subset. Many books will not have a match.

    Args:
        inventory_df: SPL inventory data
        goodreads_df: Goodreads data

    Returns:
        - Inventory with goodreads dataframe
        - Columns:
          - bib_num
          - isbns
          - raw_title
          - raw_author
          - raw_publication_year
          - raw_publisher
          - raw_subjects
          - gr_title
          - gr_authors
          - gr_publication_date
          - gr_average_rating
          - gr_ratings_count
          - gr_text_reviews_count
    """
    # Since there can be multiple ISBNs associated with a bib number
    # we need to expand the ISBNs into separate rows to match against
    # the Goodreads data
    book_isbn_df = inventory_df.select(
        F.explode(F.split(inventory_df.isbns, ", ")).alias("spl_isbn"),
        inventory_df.bib_num.alias("spl_bib_num"),
    )

    with_goodreads_df = (
        book_isbn_df
        .join(goodreads_df, (goodreads_df.isbn == book_isbn_df.spl_isbn) | (goodreads_df.isbn13 == book_isbn_df.spl_isbn))
        # If there are multiple editions, we're going to take one matching Goodreads book
        .drop_duplicates(["spl_bib_num"])
    )

    return (
        inventory_df
        .join(with_goodreads_df, with_goodreads_df.spl_bib_num == inventory_df.bib_num, how="left")
        .select(
            inventory_df["*"],
            with_goodreads_df.title.alias("gr_title"),
            with_goodreads_df.authors.alias("gr_authors"),
            with_goodreads_df.publication_date.alias("gr_publication_date"),
            with_goodreads_df.average_rating.alias("gr_average_rating"),
            with_goodreads_df.ratings_count.alias("gr_ratings_count"),
            with_goodreads_df.text_reviews_count.alias("gr_text_reviews_count"),
        )
    )


def create_fact_spl_book_checkouts(
    checkouts_df: DataFrame,
    weather_df: DataFrame,
    bib_num_publisher_lookup_df: DataFrame,
    publishers_df: DataFrame,
    output_path: str,
):
    """Creates fact_spl_book_checkout table in parquet format

    Args:
        checkouts_df: SPL checkouts data
        weather_df: Weather data
        bib_num_publisher_lookup_df: Bib number, publisher, and normalized key
        publishers_df: Publisher data
    """
    checkouts_df = link_temperature_to_checkouts(checkouts_df, weather_df)
    checkouts_df = link_publisher_id_to_checkouts(checkouts_df, bib_num_publisher_lookup_df, publishers_df)
    checkouts_df = checkouts_df.withColumn(
        "id",
        F.md5(F.concat_ws("=", checkouts_df.bib_num, checkouts_df.item_barcode, checkouts_df.checkout_datetime).alias("id")),
    )
    output_parquet(checkouts_df, output_path)


def create_dim_subjects(books_df: DataFrame, dim_output_path: str, br_output_path: str):
    """Creates dim_subject and br_book_subject tables in parquet format

    Since a book can be linked to multiple subjects, we will need to generate both a bridge
    table and a dimension table.

    Args:
        books_df: SPL inventory with Goodreads data
        dim_output_path: Path to export dimension table. Can be S3, local, etc
        br_output_path: Path to export bridge table. Can be S3, local, etc
    """
    dim_subjects_df = generate_subjects(books_df)
    output_parquet(dim_subjects_df, dim_output_path)

    br_book_subjects = generate_book_subjects(books_df, dim_subjects_df)
    output_parquet(br_book_subjects, br_output_path)


def generate_subjects(books_df: DataFrame) -> DataFrame:
    """Generates book subjects from inventory

    Args:
        books_df: SPL inventory with Goodreads data

    Returns:
        - A unique set of subjects across all books in the inventory
        - Will include an autogenerated primary key ID
        - Columns: id, subject
    """
    df = (
        books_df
        # The subjects column can contain multiple subjects separated by commas
        .select(F.explode(F.split(books_df.raw_subjects, ", ")).alias("subject"))
        .drop_duplicates(["subject"])
        .sort(F.col("subject"))
    )

    # Since the number of subjects is a reasonable number, we can create a
    # sequentially incrementing ID column by using one partition
    return df.coalesce(1).select(
        F.monotonically_increasing_id().alias("id"),
        df.subject,
    )


def generate_book_subjects(books_df: DataFrame, subjects_df: DataFrame) -> DataFrame:
    """Makes a bridge table to connect a book to multiple subjects

    Args:
        books_df: SPL inventory with Goodreads data
        subjects_df: SPL subjects

    Returns:
        - Bridge table referencing bib number to subject IDs
        - Columns: bib_num, subject_id
    """
    book_subjects_df = (
        books_df
        # The subjects column can contain multiple subjects separated by commas
        .select(
            books_df.bib_num,
            F.explode(F.split(books_df.raw_subjects, ", ")).alias("subject"),
        )
    )

    return (
        book_subjects_df
        .join(subjects_df, subjects_df.subject == book_subjects_df.subject)
        .select(
            book_subjects_df.bib_num,
            subjects_df.id.alias("subject_id"),
        )
    )


def create_dim_authors(books_df: DataFrame, dim_output_path: str, br_output_path: str):
    """Creates dim_author and br_book_author tables in parquet format

    Since a book can be linked to multiple authors, we will need to generate both a bridge
    table and a dimension table.

    Args:
        books_df: SPL inventory
        dim_output_path: Path to export dimension table. Can be S3, local, etc
        br_output_path: Path to export bridge table. Can be S3, local, etc
    """
    authors_df = generate_authors(books_df)

    dim_authors_df = authors_df.select(authors_df.id, authors_df.author.alias("name"))
    output_parquet(dim_authors_df, dim_output_path)

    br_book_subjects = generate_book_authors(books_df, authors_df)
    output_parquet(br_book_subjects, br_output_path)


def generate_authors(books_df):
    """Generates authors from inventory and linked Goodreads data

    Args:
        books_df: SPL inventory with Goodreads data

    Returns:
        - A unique set of authors across all books in the inventory
        - Will include an autogenerated primary key ID
        - Columns:
          - id: Autogenerated primary key ID
          - author: Raw author name if from Goodreads or formatted if from SPL
          - key: Normalized author is a used as join key for reducing duplicates
    """
    # Custom functions to help with formatting
    format_spl_author_udf = create_format_spl_author_udf()
    normalize_text_udf = create_normalize_text_udf()

    spl_authors_df = (
        books_df
        # Attempt to reformat the SPL authors into first name and last name.
        # The current implementation is a naive approach that only handles
        # the common cases. False positives will occur.
        .select(format_spl_author_udf(books_df.raw_author).alias("author"))
    )
    gr_authors_df = (
        books_df
        # The goodreads author data can contain multiple authors
        .select(F.explode(F.split(books_df.gr_authors, ", ")).alias("author"))
    )
    authors_df = spl_authors_df.union(gr_authors_df).dropna()

    norm_authors_df = (
        authors_df
        .select(
            authors_df.author,
            # The normalized author will be used for joining raw authors from the inventory data
            normalize_text_udf(authors_df.author).alias("key"),
        )
        .drop_duplicates(["key"])
    )

    # Since the number of authors is a reasonable number, we can create a
    # sequentially incrementing ID column by using one partition
    return norm_authors_df.coalesce(1).withColumn("id", F.monotonically_increasing_id())


def generate_book_authors(books_df, authors_df):
    """Makes a bridge table to connect a book to multiple authors

    Args:
        books_df: SPL inventory with Goodreads data
        authors_df: SPL/Goodreads authors

    Returns:
        - Bridge table referencing bib number to author IDs
        - Columns: bib_num, author_id
    """
    # Custom functions to help with formatting
    format_spl_author_udf = create_format_spl_author_udf()
    normalize_text_udf = create_normalize_text_udf()

    spl_authors_df = (
        books_df
        .select(
            books_df.bib_num,
            format_spl_author_udf(books_df.raw_author).alias("author"),
        )
    )
    gr_authors_df = (
        books_df
        .select(
            books_df.bib_num,
            F.explode(F.split(books_df.gr_authors, ", ")).alias("author"),
        )
    )
    book_authors_df = spl_authors_df.union(gr_authors_df).dropna()
    book_authors_df = book_authors_df.withColumn("key", normalize_text_udf(book_authors_df.author))

    return (
        book_authors_df
        .join(authors_df, authors_df.key == book_authors_df.key)
        .select(
            book_authors_df.bib_num,
            authors_df.id.alias("authors_id"),
        )
    )


def create_dim_publishers(
    books_df: DataFrame,
    publishers_map_df: DataFrame,
    output_path: str
) -> Tuple[DataFrame, DataFrame]:
    """Creates dim_publisher table in parquet format

    Since a book can be linked to multiple authors, we will need to generate both a bridge
    table and a dimension table.

    Args:
        books_df: SPL inventory
        publishers_map_df: Map of raw publishers to "official" publishers
        output_path: Path to export dimension table. Can be S3, local, etc

    Returns:
        We need bib_num_publisher_lookup_df and publishers_df to link publisher_id to checkouts
    """
    bib_num_publisher_lookup_df = generate_bib_num_publisher_lookup_table(books_df, publishers_map_df)
    publishers_df = generate_publishers(bib_num_publisher_lookup_df)

    dim_publishers_df = publishers_df.select(publishers_df.id, publishers_df.publisher.alias("name"))
    output_parquet(dim_publishers_df, output_path)

    return bib_num_publisher_lookup_df, publishers_df


def generate_bib_num_publisher_lookup_table(books_df: DataFrame, publishers_map_df: DataFrame) -> DataFrame:
    """Generates a look up table that can map bib_nums and publishers

    Args:
        book_df: SPL book data with Goodreads data
        publishers_map_df: Official publishers map data

    Returns:
        - A look up table between bib number and publisher
        - Columns:
          - bib_num
          - publisher: Official publisher or raw publisher
          - key: Normalized publisher to remove duplicates
    """
    normalize_text_udf = create_normalize_text_udf()
    return (
        books_df
        .join(publishers_map_df, publishers_map_df.publisher == books_df.raw_publisher, how="left")
        .select(
            books_df.bib_num,
            F.coalesce(publishers_map_df.official_publisher, books_df.raw_publisher).alias("publisher"),
            normalize_text_udf(F.coalesce(publishers_map_df.official_publisher, books_df.raw_publisher)).alias("key")
        )
    )


def generate_publishers(bib_num_publisher_lookup_df: DataFrame) -> DataFrame:
    """Generates publishers list from bib_num, publisher lookup table

    Args:
        bib_num_publisher_lookup_df: Bib number, publisher, and normalized key

    Returns:
        - Unique list of publishers with generated primary key
        - Columns:
          - id: Autogenerated primary key ID
          - publisher: Cleaned up official publisher or raw publisher
          - key: Normalized publisher to remove duplicates
    """
    clean_publisher_udf = create_clean_publisher_udf()
    return (
        bib_num_publisher_lookup_df
        .drop_duplicates(["key"])
        .coalesce(1)
        .select(
            # Since the number of publishers is a reasonable number, we can create a
            # sequentially incrementing ID column by using one partition
            F.monotonically_increasing_id().alias("id"),
            clean_publisher_udf(bib_num_publisher_lookup_df.publisher).alias("publisher"),
            bib_num_publisher_lookup_df.key,
        )
    )


def create_dim_checkout_datetimes(book_checkouts_df: DataFrame, output_path: str):
    """Creates dim_checkout_time table in parquet format

    Args:
        book_checkouts_df: SPL book checkouts
        output_path: Path to export parquet data. Can be S3, local, etc
    """
    dim_checkout_time_df = generate_checkout_times(book_checkouts_df)
    output_parquet(dim_checkout_time_df, output_path)


def generate_checkout_times(book_checkouts_df: DataFrame) -> DataFrame:
    """Generates checkout time data from book checkouts

    Args:
        book_checkouts_df: SPL book checkouts

    Returns:
        Creates a time dimension from checkout times with hour, day, week,
        weekday, month, and year extracted
    """
    checkout_times_df = (
        book_checkouts_df
        .select(book_checkouts_df.checkout_datetime)
        # Checkouts can occur at the same time since a patron can check out multiple books
        # In addition, there are multiple library locations and checkout stations
        .drop_duplicates(["checkout_datetime"])
    )

    return checkout_times_df.select(
        checkout_times_df.checkout_datetime,
        F.hour(checkout_times_df.checkout_datetime).alias("hour"),
        F.dayofmonth(checkout_times_df.checkout_datetime).alias("day"),
        F.weekofyear(checkout_times_df.checkout_datetime).alias("week"),
        F.dayofweek(checkout_times_df.checkout_datetime).alias("weekday"),
        F.month(checkout_times_df.checkout_datetime).alias("month"),
        F.year(checkout_times_df.checkout_datetime).alias("year"),
    )


def create_dim_books(books_df: DataFrame, output_path: str):
    """Creates books dimension in parquet format

    Args:
        book_df: SPL book data with Goodreads data
        output_path: Path to export dimension table. Can be S3, local, etc
    """
    dim_book_df = generate_books(books_df)
    output_parquet(dim_book_df, output_path)


def generate_books(books_df):
    """Generates book data from inventory

    Args:
        book_df: SPL book data with Goodreads data

    Returns:
        Books dimension
    """
    clean_title_udf = create_clean_title_udf()
    clean_publication_year_udf = create_clean_publication_year_udf()
    return books_df.select(
        books_df.bib_num,
        clean_title_udf(books_df.raw_title, books_df.gr_title).alias("title"),
        books_df.isbns,
        clean_publication_year_udf(books_df.raw_publication_year, books_df.gr_publication_date).alias("publication_year"),
        books_df.gr_average_rating.alias("average_rating"),
        books_df.gr_ratings_count.alias("ratings_count"),
        books_df.gr_text_reviews_count.alias("text_reviews_count"),
        books_df.raw_title,
        books_df.raw_author,
        books_df.raw_publisher,
        books_df.raw_publication_year,
    )


def link_temperature_to_checkouts(checkouts_df: DataFrame, weather_df: DataFrame) -> DataFrame:
    """Links temperature for the day to SPL checkout data

    Args:
        checkouts_df: SPL checkouts data
        weather_df: Weather data

    Returns:
        Checkouts with temperature
    """
    return (
        checkouts_df
        .join(
            weather_df,
            (weather_df.month == F.month(checkouts_df.checkout_datetime)) &
            (weather_df.day == F.dayofmonth(checkouts_df.checkout_datetime)) &
            (weather_df.year == F.year(checkouts_df.checkout_datetime)),
            how="left",
        )
        .select(
            checkouts_df["*"],
            weather_df.temperature,
        )
    )


def link_publisher_id_to_checkouts(
    checkouts_df: DataFrame,
    bib_num_publisher_lookup_df: DataFrame,
    publishers_df: DataFrame,
) -> DataFrame:
    """Links publisher ID to SPL checkouts data

    Args:
        checkouts_df: SPL checkouts data
        bib_num_publisher_lookup_df: Bib number, publisher, and normalized key
        publishers_df: Publisher data

    Returns:
        Checkouts with publisher ID
    """
    return (
        checkouts_df
        .join(bib_num_publisher_lookup_df, bib_num_publisher_lookup_df.bib_num == checkouts_df.bib_num, how="left")
        .join(publishers_df, publishers_df.key == bib_num_publisher_lookup_df.key, how="left")
        .select(
            checkouts_df["*"],
            publishers_df.id.alias("publisher_id"),
        )
    )


def create_clean_title_udf():
    """UDF to clean title text

    - Prefer title from Goodreads since it is well formatted
    - The SPL title includes author/illustrator information typically
      split by a slash
    - It seems like it could be possible for a title to have a slash
      in the title, but in version 1, we'll take a naive approach

    Examples:
        - The only child : a novel / Andrew Pyper.
        - The great powers outage / William Boniface ; illustrations by Stephen Gilpin.
    """
    def clean_title_text(spl_title, gr_title):
        if gr_title:
            return gr_title

        if not spl_title:  # Possible for title to be null
            return None
        try:
            title, _ = spl_title.rsplit(" / ", 1)  # Naively split on first slash to the right
            return title.strip()
        except ValueError:
            # There are cases where the title does not contain a slash
            return spl_title

    return F.udf(clean_title_text)


def create_clean_publication_year_udf():
    """UDF to clean publication year

    - Prefer publication date from Goodreads since it is well formatted
    - The SPL data has inconsistent formatting of the year
    - We can extract the year with regex that grabs four consecutive digits
    - If there are multiple digits, we'll take the smallest
    - Examples:
      - 2008.
      - [2014]
      - ©2014
      - 1991, c1988.
      - 2003, c1999.
    """
    def clean_publication_year_text(spl_publication_year, gr_publication_date):
        if gr_publication_date:
            return gr_publication_date.year

        if not spl_publication_year:  # Possible for publication year to be null
            return None

        years = sorted([int(year) for year in re.findall(r"(\d{4})", spl_publication_year)])
        return years[0] if years else None

    return F.udf(clean_publication_year_text)


def create_clean_publisher_udf():
    """UDF to clean publisher text

    For some reason there's a trailing comma after many of the publishers
    in the SPL inventory data.
    """
    def clean_publisher_text(publisher):
        if not publisher:  # Possible for publisher to be null
            return None
        return publisher.strip().strip(",")

    return F.udf(clean_publisher_text)


def create_format_spl_author_udf():
    """UDF to format SPL author

    Examples of SPL inventory authors format:

        - Brand Miller, Janette, 1952-
        - Cabatingan, Erin
        - Berghahn, Volker R. (Volker Rolf), 1938-
        - Avi, 1937-
        - Lagerlöf, Selma, 1858-1940
        - United States. Maritime Commission

    The formatting is inconsistent.

    - The safest formatting we can do is to remove the year suffixes
    - If the year suffix exists, we can swap last name and first name to
      better match the Goodreads format.
    - If there is only 1 comma, we can try to swap the last name and first name
      if the strings are a reasonable length.
    """
    def format_spl_author(author):
        if author is None:
            return author

        author_no_years = re.sub(r", \d{4}-(\d{4})?$", "", author)

        # Handle case where a year suffix has been found.
        #
        # In this case we can be more confident about swapping the last/first names
        if len(author_no_years) != len(author):
            author_parts = author_no_years.split(", ")
            if len(author_parts) == 2:
                # Only swap last_name, first name when there are two parts
                return "{} {}".format(author_parts[1], author_parts[0])

            # We were able to remove the year, but did not find the expected number
            # commas. This means that the author may have go by one name. Or this
            # is an unknown format.
            return author_no_years

        # In the case where a year suffix has not been found, we'll be more careful.
        # We'll only perform the swap if both name parts are under 36 characters.
        author_parts = author.split(", ")
        max_name_len = 36
        if len(author_parts) == 2 and len(author_parts[0]) < max_name_len and len(author_parts[1]) < max_name_len:
            return "{} {}".format(author_parts[1], author_parts[0])

        return author

    return F.udf(format_spl_author)


def create_normalize_text_udf():
    """UDF to normalize text to reduce duplicates

    This is a naive approach to normalizing the text. Duplicates will be missed,
    but we will try to avoid false positives (incorrectly merging duplicates).
    """
    def normalize_text(text):
        if text is None:
            return text

        # Lowercase
        text = text.lower()

        # Remove text in parenthesis
        text = re.sub(r"(\s*\(.+\)\s*)$", "", text)

        # Remove punctuation and digits
        text = re.sub(r"[\[\]<>{}|\\!\"#&()*+,./:;?@_-]", " ", text)

        # Remove apostrophe (special case since we don't want a space here)
        text = re.sub(r"'", "", text)

        # Remove extra spaces
        return re.sub(r"\s{2,}", " ", text).strip()

    return F.udf(normalize_text)


def output_parquet(df, output_path, partition_by=None):
    """Writes dataframe in parquet format

    Args:
        df: Dateframe to write
        output_path: Can be S3 bucket, HDFS, or local filepath
        partition_by: Optionally, columns on dateframe to partition by
    """
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(output_path)


if __name__ == "__main__":
    main()
