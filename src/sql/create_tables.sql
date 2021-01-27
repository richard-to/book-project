CREATE TABLE IF NOT EXISTS dim_book (
    bib_num VARCHAR(10) PRIMARY KEY SORTKEY,
    title VARCHAR(1500),
    isbns VARCHAR(1500),
    publication_year INT,
    avg_rating FLOAT,
    ratings_count INT,
    text_reviews_count INT,
    raw_title VARCHAR(1500),
    raw_author VARCHAR(255),
    raw_publisher VARCHAR(500),
    raw_publication_year VARCHAR(255)
) DISTSTYLE all;

CREATE TABLE IF NOT EXISTS dim_author (
    id BIGINT PRIMARY KEY,
    name VARCHAR(1000) NOT NULL SORTKEY
) DISTSTYLE all;

CREATE TABLE IF NOT EXISTS dim_subject (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL SORTKEY
) DISTSTYLE all;

CREATE TABLE IF NOT EXISTS dim_publisher (
    id BIGINT PRIMARY KEY,
    name VARCHAR(500) SORTKEY
) DISTSTYLE all;

CREATE TABLE IF NOT EXISTS dim_checkout_time (
    checkout_datetime TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY DISTKEY SORTKEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    weekday INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL
);

CREATE TABLE IF NOT EXISTS br_book_author (
    bib_num VARCHAR(10) SORTKEY,
    author_id BIGINT NOT NULL REFERENCES dim_author (id)
) DISTSTYLE all;

CREATE TABLE IF NOT EXISTS br_book_subject (
    bib_num VARCHAR(10) SORTKEY,
    subject_id BIGINT NOT NULL REFERENCES dim_subject (id)
) DISTSTYLE all;

CREATE TABLE IF NOT EXISTS fact_spl_book_checkout (
    bib_num VARCHAR(10) NOT NULL,
    item_barcode VARCHAR(13),
    checkout_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL REFERENCES dim_checkout_time (checkout_datetime) DISTKEY SORTKEY,
    temperature FLOAT,
    publisher_id BIGINT REFERENCES dim_publisher (id),
    id VARCHAR(32) PRIMARY KEY
);
