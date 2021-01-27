# Publisher Mapping

Since I didn't want to install Elasticsearch on AWS, I used my local Elasticsearch
installation. You'll need to have Elasticsearch installed to make this work.

You'll need to set `elasticsearch_host` in `etl.cfg`. This is located in the `publishers`
section.

Also make sure to download local copies of the SPL data dictionary and inventory CSV files.
You'll need to temporarily update the paths in `etl.cfg`.

The specific paths are `data_dict_path` and `inventory_path` under the `spl` section.

The end result is a CSV file that maps raw publishers in the SPL inventory to publisher
in the Wikipedia list.

## Steps:

- Scrape the Wikipedia publishers page
    - `python scrape_publishers.py`
    - This generate a `publishers.csv` file
- Load the scraped publishers into Elasticsearch
    - `python create_publishers_search_index.py`
- Create publishers mapping
   - `python create_publishers_mapping.py`
   - This will generate a directory called `publishers_data`
   - Inside that directory will be a CSV file with the publisher mappings
- Upload data to S3
  - Upload this file to S3
- Update `etl.cfg`
  - Update the `data_path` under the publishers section with the S3 location of this file
