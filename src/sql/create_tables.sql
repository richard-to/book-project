CREATE TABLE IF NOT EXISTS dim_book (
    bib_num VARCHAR(255) PRIMARY KEY SORTKEY,
    title VARCHAR(255),
    isbns VARCHAR(255),
    publication_year INT,
    avg_rating FLOAT,
    ratings_count INT,
    text_reviews_count INT,
    raw_title VARCHAR(255),
    raw_author VARCHAR(255),
    raw_publisher VARCHAR(255),
    raw_publication_year VARCHAR(255)
) DISTSTYLE all;

CREATE TABLE IF NOT EXISTS dim_author (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL SORTKEY
) DISTSTYLE all;

CREATE TABLE IF NOT EXISTS dim_subject (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL SORTKEY
) DISTSTYLE all;

CREATE TABLE IF NOT EXISTS dim_publisher (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL SORTKEY
) DISTSTYLE all;

CREATE TABLE IF NOT EXISTS dim_checkout_time (
    checkout_datetime TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY DISTKEY SORTKEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
);

CREATE TABLE IF NOT EXISTS br_book_author (
    bib_num VARCHAR(255) NOT NULL REFERENCES fact_spl_book_checkout (bib_num) SORTKEY,
    author_id INT NOT NULL REFERENCES dim_author (id)
) DISTSTYLE all;

CREATE TABLE IF NOT EXISTS br_book_subject (
    bib_num VARCHAR(255) NOT NULL REFERENCES fact_spl_book_checkout (bib_num) SORTKEY,
    subject_id INT NOT NULL REFERENCES dim_subject (id)
) DISTSTYLE all;

CREATE TABLE IF NOT EXISTS fact_spl_book_checkout (
    id VARCHAR(32) PRIMARY KEY,
    bib_num INT NOT NULL,
    publisher_id INT REFERENCES dim_publisher (id),
    checkout_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL REFERENCES dim_checkout_time (checkout_datetime) DISTKEY SORTKEY,
    item_barcode VARCHAR(15),
    temperature FLOAT
);
