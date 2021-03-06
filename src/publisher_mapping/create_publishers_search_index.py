"""
Loads "official" publisher data into elasticsearch to help with de-duplicating
raw publishers.
"""
import configparser

from elasticsearch import Elasticsearch
import pandas

config = configparser.ConfigParser()
config.read("../etl.cfg")

ES_HOST = config.get("publishers", "elasticsearch_host")

ES_INDEX_NAME = "book_publishers"
ES_DOC_TYPE = "publisher_name"

PUBLISHERS_CSV = "publishers.csv"  # This file is generated by scrape_publishers.py


def main():
    es = Elasticsearch([ES_HOST])

    if es.indices.exists(ES_INDEX_NAME):
        drop_index(es)

    create_index(es)

    populate_index(es)


def drop_index(es):
    es.indices.delete(index=ES_INDEX_NAME)


def create_index(es):
    es.indices.create(index=ES_INDEX_NAME, body={
        "settings": {
            "analysis": {
                "analyzer": {
                    "default": {
                        "type": "snowball",
                        "language": "English"
                    }
                }
            }
        }
    })

    es.indices.put_mapping(index=ES_INDEX_NAME, doc_type=ES_DOC_TYPE, body={
        "properties": {
            "publishers": {
                "type": "text",
            },
        },
    })


def populate_index(es):
    publishers_df = pandas.read_csv(PUBLISHERS_CSV, skip_blank_lines=True)
    for index, row in publishers_df.iterrows():
        es.index(index=ES_INDEX_NAME, doc_type=ES_DOC_TYPE, id=index, body={"publisher": row.publisher})


if __name__ == "__main__":
    main()
