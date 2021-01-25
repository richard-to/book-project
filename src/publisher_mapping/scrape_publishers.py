"""
Script to scrape a list of publishers from Wikipedia

- Unfortunately this list contains US publishers only
- This list will serve as a gazetteer for "official" publisher names
"""
from lxml import etree
import pandas
import requests


EXPECTED_NUM_LIST_OF_LINKS = 8
CSV_OUTPUT_FILE = "publishers.csv"
PUBLISHERS_URL = "https://en.wikipedia.org/wiki/List_of_English-language_book_publishing_companies"


def main():
    # Retrieve publishers HTML page
    r = requests.get(PUBLISHERS_URL)
    root = etree.fromstring(r.text, etree.HTMLParser())

    # Scrape HTML
    num_list_of_links = 0
    publishers = []
    # - Publishers are located in divs with class called "div-col"
    # - Each list element contains a publisher link, but may contain other links
    #   - e.g. [Microsoft Press] – a publishing arm of [Microsoft]
    # - The first link is always the publisher
    for row in root.xpath(".//div[@class='div-col']/ul/li/a[1]"):
        publisher = row.text
        if publisher.startswith("List of "):
            # This xpath gets all publishers except eight links that link to other lists at the end.
            num_list_of_links += 1
            continue
        publishers.append(publisher)

    assert_msg = f"Encountered an unexpected number of extra links ({num_list_of_links}",
    assert num_list_of_links == EXPECTED_NUM_LIST_OF_LINKS, assert_msg

    # Write to CSV
    publishers_df = pandas.DataFrame(publishers, columns=["publisher"])
    publishers_df.to_csv(CSV_OUTPUT_FILE, index=False)


if __name__ == "__main__":
    main()
