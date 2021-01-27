# SPL Book Checkouts Project

## 0. Usage instructions

For usage instructions, see docs/usage.md.

## 1. Scope the Project and Gather Data

### Use case

The Seattle Public Library  (SPL) checkout data spans from 2005 to 2017. In that time there were almost 50 million checkouts of print books. Due to the large amount of data it is difficult analyze the data on my laptop. This is why I would like to create a star schema and load the data into Redshift, so I can run SQL queries against the data.

### Data sources

- https://www.kaggle.com/seattle-public-library/seattle-library-checkout-records
	- CSV format
- https://www.kaggle.com/jealousleopard/goodreadsbooks
	- CSV format
- https://academic.udayton.edu/kissock/http/Weather/gsod95-current/WASEATTL.txt
	- Formatted text file
- https://en.wikipedia.org/wiki/List_of_English-language_book_publishing_companies
	- HTML web page (need to scrape)

## 2. Explore and Assess the Data
The following sections contain an abridged summary of my exploration/analysis of the data. I analyzed subsets of the data using my local Spark installation. The Jupyter notebooks can be viewed in the nbs folder. Note that the code there is adhoc and a bit messy.

In addition to Spark, I also used Kaggle's data viewer.

### 2.1 SPL Checkout data

The SPL checkout data contains three types of files:

- Data dictionary
	- Integrated\_Library\_System\_\_ILS\_\_Data_Dictionary.csv
- Inventory
	- Library\_Collection\_Inventory.csv
- Checkouts
	- The checkout data is partitioned by year
	- Examples:
		- Checkouts\_By\_Title\_Data\_Lens\_2005.csv
		- Checkouts\_By\_Title\_Data\_Lens\_2017.csv

#### 2.1.1 Data dictionary

The data dictionary data is clean. It gives us an idea of what types of items the SPL contains. Since we only care about books, we can use the data dictionary to filter out books only.

Example item types:

- Framed Art: Adult/YA
- Book: Adult/YA
- Audio Tape: Adult/YA
- CD: Adult/YA
- CD-ROM: Adult/YA

We can filter out print books by filtering on the following columns:

- CodeType = ItemType
- FormatGroup = Print
- FormatSubGroup = Book

This leaves us with twelve types of item types. There are some types in this filtered set that we may want to exclude, such as song books and uncatalogued material. Each item type is associated with a code (e.g.  acbk, ahbk, bcbk, etc) that are used as foreign keys on the other two files. So we could filter records using a whitelist of codes. However for this project, I will be filtering by what the data dictionary has defined as "print books."

- Book: Adult/YA
- Book: Adult/YA: No Hold
- Book: Adult/Formerly No Fine
- Book: Dummy Record
- Uncataloged Material
- Part of Book: Analytic Record
- Book: Ref Adult/YA
- Book: Juv
- Song Book: Bound Juv
- Uncataloged Juv Mat
- Book: Ref Juv
- Song Book: Ref Juv

#### 2.1.2 Inventory

The inventory data has many columns with inconsistent formatting, as if they were manually entered by hand. In this section I will point out a few inconsistencies that I found in my analysis of this data.

##### 2.1.2.1 Bib Number

The same bib number can appear multiple times and some cases hundreds of times in the inventory. This means that the bib number may not be an identifier for a item.

The bib number seems to point to the same book, but what differs are ItemType, ItemCollection, and ItemLocation. This is especially true of ItemLocation. Unfortunately we can't use the combination of these four columns to uniquely match an item in the checkout CSVs since that data is missing the ItemLocation.

For this reason I have taken the liberty of removing duplicate bib number rows since they appear to represent the same book.

##### 2.1.2.2 Title

The titles contain more than the title of the book. Often times author, illustrator, and editor information is included.

Examples:

- The Paris pilgrims : a novel / Clancy Carlile.
- Big Bill Haywood and the radical union movement [by] Joseph R. Conlin.
- Japanese arms & armor. Introd. by H. Russell Robinson

There are some heuristics we can make in terms of removing non-title parts of the text. A naive approach could be to split by `/` or `[by]`.  Those seem somewhat safe, but not foolproof.

##### 2.1.2.2 Author

The author data is a bit cleaner. But there are still some inconsistencies and oddities in the format.

Examples:

- Brand Miller, Janette, 1952-
- Cabatingan, Erin
- Berghahn, Volker R. (Volker Rolf), 1938-
- Avi, 1937-
- Lagerlöf, Selma, 1858-1940
- United States. Maritime Commission

In this case, it would be good to remove the years and to also reverse the order the names for consistency. The Goodreads data that we will explore later formats authors by first name and last name and doesn't include the year that the author was born and/or died.

We can try out best to normalize this data by splitting the author column by comma. Since commas may appear for other reasons, we want to ensure that what we're splitting are actually names. One heuristic we could use is that names are often relatively short. So we can check if both the first name and last name are each under say 36 characters.

#### 2.1.2.3 ISBNs

Each format  (e.g. paperback, hardback) and edition will have a unique ISBN. This means that the same book can have multiple ISBNs. In addition a book can contain both an ISBN10 and an ISBN13.  A Bib number seems to encompass a book, so that means a library patron can check out a book (i.e. bib number) but it may be a hardback or a paperback.

The ISBNs are stored in one column and are separated by commas.

The ISBNs will be useful for matching Goodreads data with the SPL inventory.

#### 2.1.2.3 PublicationYear

Publication year is another inconsistently formatted column. In addition it's formatted in such a way that we cannot run a query to check the number of books that the SPL has that were published at a given year.

Examples:

      - 2008.
      - [2014]
      - ©2014
      - 1991, c1988.
      - 2003, c1999.
      - 114 So. Washington, Orting 98360-0040)

The best we can do is use a regex to extract anything in the text that is four digits long. Then if there are multiple years found, we will take the earliest year. Intuitively the earliest year should be the first edition of the book.

#### 2.1.2.4 Publisher

The publisher data like the other columns in the inventory data is very inconsistent. Also for some reason, there's often a trailing comma after the publisher.

For instance, there are many variations of Schocken Books. These variations in spelling prevent us from asking questions such as how many books in the inventory does the SPL have by X publisher.

Example of Schocken Books variations (note: that the Spark show command cutoff some of the output here, hence the ellipses):

- Schocken : Distri...
- Schocken : Nextbook,
- Schocken Books
- Schocken Books : ...
- Schocken Books : ...
- Schocken Books ; ...
- Schocken Books ; ...
- Schocken Books,
- Schocken Books, O...
- Schocken Publishi...
- Schocken

Cleaning up the Publisher column will be very hard. I attempted to resolve the duplicate by grabbing a list of publishers from Wikipedia. Unfortunately this contained only US publishers.

I tried to match up the SPL data with the Wikipedia data when possible. To do this, I stored the Wikipedia publishers data in Elasticsearch. Since Elasticsearch won't always return the right match, I also used the fuzzywuzzy library (fuzzy matching library) to double check that the SPL publisher and the Wikipedia publisher matches were at least a 90% match. The overall matches were OK. There were false positives and also a lot of publishers that had no match at all.

Example matches:

- Bloomsbury, -> Bloomsbury Publishing
- Crowell ; HarperCollins, -> HarperCollins
- Knopf : distributed by Random House, -> Random House
- American Elsevier Pub. Co. -> Elsevier
- Westminster/John Knox Press ; Saint Andrew Press -> Westminster John Knox Press
- Frick Collection in association with Yale University Press, -> Yale University Press

#### 2.1.3 Checkouts

The checkout data is partitioned by year. There is one CSV per year from 2005 to 2017.

The checkout data is pretty clean. It contains item_barcode which unique identifies a specific instance of a book. It also contains the checkout date and time, which is very valuable due to the granularity is accurate up the minute. The seconds are all set to 0.

The only oddity here is that it's unclear how one would match up the bib number with the inventory since the bib number can appear many times.

### 2.2 Goodreads data

The Goodreads is a supplemental dataset. I plan to join this data with the SPL inventory data. We can do this using the ISBN/ISBN13 columns.

The addition of the Goodreads data could be helpful since it includes the average Goodreads rating of book and the number of reviews. This allows us to ask questions based on popularity. Are popular books more likely to be checked out?

The one caveat that I found is that this dataset is only a subset of the full Goodreads database. Most of the books are in this dataset are from 2000-2010. This means this dataset won't be as helpful as it could be. We likely won't get too many matches.

![Distribution of publication year in Goodreads data](images/gr_pub_year_hist.png?raw=true)

The Goodreads data is also noticeably cleaner than the SPL data, so we could replace the title, author, publisher, and/or publication year with the Goodreads version.

#### 2.2.1 Publisher

Since the publisher data shows some duplicates similar to the SPL data, I decide to not use the Goodreads publisher data to keep things simpler.

Examples:

- Schirmer Mosel
- Schirmer/Mosel
- Schocken
- Schocken Books
- Schocken Books Inc
- Scholastic Books
- Scholastic Inc
- Scholastic Inc.
- Scholastic Nonfic...
- Scholastic Paperb...
- Scholastic Press

#### 2.2.2 ISBN/ISBN13

The data here is mostly clean. There are some errors, but it's a huge issue since they won't be matched to any bib number in the SPL data.

ISBN10 has a few non ISBNs:

- 3.63
- 0.00
- 3.58
- 3.58
- 084386874 (only 9 digits)

ISBN13 has a few cases of ISBN10s:

- 156384155X
- 0851742718
- 0674842111
- 1593600119

#### 2.2.3 Authors

The Goodreads data will list multiple authors, editors, illustrators, etc under this column. This list can get very long for anthologies. The delimiter used is slash (e.g. `/`). So we can split this to get all the authors. This data would be a nice enhancement of the SPL data which seems to only include one author.

#### 2.2.4 Title

The titles are much cleaner than the SPL data. There's no extra author information included. It's just the title as it should be. So this would be another good field to use as a supplement for the SPL data.

#### 2.2.5 Publication date

This is another field which is much cleaner than the SPL counterpart since it is a properly formatted date. We can use this field to supplement the SPL PublicationYear field.

### 2.3 Seattle weather data

Despite the odd formatting (i.e. each column is separated by a varying amount of spaces), this data is relatively clean and easy to use. It contains four columns: Year, month, day, and temperature in Fahrenheit.

I downloaded just the weather data for Seattle. Then we can link this data to the checkout dates so we can ask questions such as, do people tend to check out books on hot days or cold days.

It would better if this data included whether it was rainy or cloudy or sunny, but that's OK.

### 2.4 Wikipedia publisher data

I plan to use the Wikipedia publisher data to help link the SPL publisher data to a normalized set of publishers. This way we can limit some of the duplicates.

The drawback to this data is that it is only US publishers. Wikipedia also has some pages that link ISBNs to publishers.  How this works is that the first X digits of an ISBN are allocated to a publisher. Then the rest of the ISBN represents a specific book by the publisher. So theoretically you could determine the publisher using the ISBN.

But for simplicity, I scraped the publishers using requests and lxml.

## 3: Define the Data Model

### 3.1 Redshift Schema

I will be using a basic star schema to make querying the data easy and familiar.

I will be using a transactional fact table that includes rows for each book checkout since this gives us the most granularity and flexibility.

There will be a time dimension based on the checkout date time. This will make it easier to run queries to roll the the data up by month and year.

There will be book dimension so we can get some book information. If I was able to link Goodreads data to the the SPL book, then we will also have access to some ratings data.

Publisher, author, and subject will be separate dimensions. Since a book can have multiple authors and subjects, these dimensions will also have bridge tables to allow those relationships to work.

The SPL publisher and author data is not very clean, so we will do what we can to eliminate duplicates. The better we can do this the more effective. queries against these dimensions will be.

The SPL subject data is not the best. There are 416,937 unique subjects which makes the data less useful. But imagine there's a low tail of subjects with only one mention.

![Star schema for SPL checkouts data](images/star_schema.png?raw=true)

### 3.2 Data pipeline

- Manually upload raw data to S3
- Use Spark to clean and transform the data into the schema from section 3.1
- Save the data tables in parquet format in S3
- Load the data tables into Redshift using the COPY command
- Run data quality checks

#### 3.2.1 Adhoc Wikipedia publishers data preparation

We will also have an adhoc step to prepare the Wikipedia publishers data that will be outside of the data pipeline.

- Scrape Wikipedia page
- Load data into Elasticsearch
- Using Spark, match SPL inventory publishers with Wikipedia publishers
- Save matches as CSV
- Upload this data to S3 along with other raw data

These adhoc steps could have been done as a part of the same pipeline, but I didn't want to set up an Elasticsearch instance on AWS for such a small data processing job.

## 4. Project Write Up

### 4.1 Goal

The primary goal of this project was to take the SPL checkout data and load it into Redshift to make the data easier to analyze the data.

The secondary goal was to normalize and clean up inconsistent data.

The third goal was to try to enrich the existing SPL inventory data with Goodreads data.

### 4.2 What queries will you want to run?

There are many types of interesting queries that could be run. Some examples:

- What are the top ten most popular book checkouts by year?
- What day of the week is the most popular for checking out books?
- What time of day is most popular for checkout books of a certain subject?
	- For instance are children's books more popular in the morning or the afternoon?
- What books have never been checked out?
- Is there some correlation between highly rated Goodreads books and number of checkouts?
- Who is the most popular author by checkouts?
- Which author has the most books in the SPL inventory?
- Does temperature have any effect on book checkouts?
- Our newer  (i.e. published in the same year) books more likely to be checked out than classics?

### 4.2 How would Spark or Airflow be incorporated?

For this project I uploaded the raw data to S3. I found that Spark's DataFrame API was great for running adhoc analysis on the data  before deciding on a set schema.  I like that the DateFrame API is similar to using Pandas.

In addition Spark was great for the data wrangling tasks where I needed to take the raw data and transform it into the star schema that I designed. The bonus of this approach is that in addition to Redshift, I can also query the star schema with Spark if I wanted to.

I preferred this approach over loading the raw data into staging tables on Redshift because of the amount of data clean up and processing I needed to perform. There were cases where I needed to use UDFs. Also for the weather data I had use RDDs to load the data (though this was because EMR doesn't have Spark 3 yet, which doesn't support multiple character delimiters).

I did not use Airflow in this project since this was a one time ETL pipeline, which I felt would add unnecessary complexity. However if I were designing a pipeline that needed to ingest data every day or month, I would have added Airflow. The reason is because of Airflow's scheduling and error handling and notification capabilities.  In addition Airflow's ability to run incremental pipelines is also very useful.

Here is what my Airflow DAG would have looked like:

- Create EMR cluster
- Add step run my spark script to create star schema
- Terminate EMR cluster
- Load star schema data (parallel by table)
- Data quality checks (parallel by table)

If I were to make the pipeline incremental, I would need to make changes to my Spark script. Specifically I would need to ensure that the primary key IDs for existing rows that I generated for dim\_author, dim\_publisher, and dim\_subject were stayed the same. Right now if new data were added, the primary key  IDs would change.

In addition, I would need to make adjustments to how data was ingested to avoid duplicates for the book, author,  publisher, and subjects.

New checkouts would be the easiest to handle since those events would always be new. Basically like an append-only log.

### 4.3 Why did you choose the model you chose?

For my model I used a simple star schema with a transactional fact table for the checkouts. I did have to add two bridge tables to handle the many to many relationship between subjects/authors and check outs. But otherwise pretty basic, which is good. I think using a familiar schema will make the data more user friendly. More people will be able to analyze the data. It would be a lot harder to analyze the SPL checkout data in raw format.

Based on the size of the data, the only tables I will be partitioning across multiple nodes are `fact_spl_book_checkout` and `dim_checkout_time`. These two tables have a  `DISTKEY` on `checkout_datetime` which should yield a good distribution of the data since there are many unique values.  The reason to partition these two tables is they have the most rows.  `fact_spl_book_checkout` has almost 50 million rows. Checkouts also have higher rate of growth at about 4 million records per year.

The other dimension tables are significantly smaller and likely have a smaller growth rate. So it made sense to use `DISTSTYLE all` for these tables.

- Books = 495,635
- Authors = 202,893
- Publishers = 62,014
- Subjects = 416,937

Here is the diagram one more time:

![Star schema for SPL checkouts data](images/star_schema.png?raw=true)

### 4.4 Clearly state the rationale for the choice of tools and technologies for the project

Much of my rationale has been in sections 4.2 and 4.3. Here is a summary:

#### 4.4.1 Spark

I chose Spark for the following use cases:

- Low risk adhoc exploratory data analysis on S3
	- Low risk meaning that I don't need to define a schema and load the data into a database
	- DataFrames work like Pandas, but more scalable
- Good for scalable complex data wrangling
- Easy to write output to S3 and also partition the data

#### 4.4.2 Redshift

I chose Redshift since it's easier to analyze data using SQL queries than adhoc Spark notebooks or python scripts. In addition, having a database allows me to add a frontend UI in the future.

With only 50 million records, I think Redshift is slight overkill. I could have used Amazon RDS instead. But for learning purposes, Redshift was a good option.

### 4.5 Propose how often the data should be updated and why

Seattle's Open Data website no longer provides checkout data at a transactional level. They now only provide a dataset that aggregates book checkouts by month. It seems they update that set every month.

So assuming that was the case for the previous data set, I'd schedule airflow to run my data pipeline once a month.

### 4.6 Document the steps of the process

- Manually upload raw data to S3
	- Download SPL data from Kaggle and upload to S3 bucket
	- Download Goodreads data from Kaggle and upload to S3 bucket
	- Download Seattle weather data and upload to S3 bucket
	- Scrape Wikipedia publishers data
		- Scrape data from Wikipedia
		- Load data into Elasticsearch
		- Use local Spark to match SPL inventory publishers with Wikipedia publishers
		- Save matches as CSV
		- Upload matches to S3 bucket
- Use Spark to clean and transform the data into the schema from section 3.1
	- Load SPL books data and filter by print books using data dictionary
	- Load Goodreads data
	- Link SPL Books data with Goodreads data
		- Need to handle multiple ISBNs in one column
	- Create subject dimension
		- Multiple subjects are stored in one column and separated by commas
		- Need separate the subjects and remove duplicates
		- Need to create a bridge table to handle the many to many relationship
	- Create author dimension
		- Use Goodreads authors if a link exists
		- Multiple Goodreads authors are stored in one column and separated by a slash
		- Need to separate the subjects
		- If no Goodreads author exists, use the SPL author
		- The SPL author field needs to be cleaned up
			- Can contain year of birth and death
			- Uses a last name, first name format. But Good reads uses a first name last name format.
		- Need to remove normalize and remove duplicates
		- Need to create a bridge table to integrate Goodreads data which can contain multiple authors
	- Create publisher dimension
		- Match up Wikipedia publishers by SPL publisher using the file we created manually
		- Since not all publishers will be matched, we need to remove duplicates using a similar technique to what we used for creating the authors dimension
	- Create book dimension
		- Clean up title. Use Goodreads version if it exists
		- Clean up year so that it is numeric. Use Goodreads version if it exists
	- Create checkout_time dimension
		- This one is self-explanatory. This will be our standard time dimension
	- Create our transactional fact table of book checkouts
		- Load weather data
			- Will need to use RDDs to parse the lines due to weird delimiter usage
		- Link temperature to checkouts based on month, day, and year
		- We will also need to link the publisher ID to the book checkout
			- This requires two joins connect the publisher ID to the checkout since the publisher column doesn't have a bib number.
- Save the data tables in parquet format in S3
	- Use coalesce to set a smaller number of partitions to increase write performance
- Run script to create Redshift cluster
- Create tables
- Load the data tables into Redshift using the COPY command
- Run data quality checks
- Terminate spark cluster once everything looks good
- Run queries
- When done, delete cluster

### 4.7 How I would approach the problem differently

#### If the data was increased by 100x

If the data increased by 100x, then we would be looking at 5 billion checkouts. We would still be able to use Redshift, but we would want to increase the instance sizes and number of nodes. This would require some tinkering and monitoring to determine the optimal size of the cluster.

The schema wouldn't need to be changed as I don't envision the book, authors, subjects, and publishers data would increase by 100x. Only the checkouts would. But the use the `DISTKEY` on `checkout_datetime` should allow for a relatively even distribution as the data grows.

We would need to increase the size of the Spark cluster as well. We would likely need to update some of the code to be more efficient. For example right now I'm writing the checkout data to parquet without any explicit partitioning. We may want to set a partition by year and month.

#### If the pipelines were run on a daily basis by 7am

Here I would definitely have used Airflow. See section 4.2.

In addition I would also need to update the Spark code to handle incremental updates to the data. There would be two use cases.

1. New books added
2. New checkouts added

This is also discussed in more detail section 4.2.

The other change would be make sure no duplicates are added to Redshift. For the checkouts scenario, it will be an append-only scenario. This means we'd need to generate a staging table in Spark with only the new checkouts data.

In addition we would also want to update the Spark version of `fact_spl_book_checkout` with the new data.

For the dimension data (excluding `dim_checkout_time`), we could probably get away with truncating and repopulating the data. Ideally we would want to ensure that the primary key IDs stay the same though.

#### If the database needed to be accessed by 100+ people

Since we're using Redshift I would enable the concurrency scaling feature (https://docs.aws.amazon.com/redshift/latest/dg/concurrency-scaling.html). This would be ideal since high usage of the database may not occur consistently for 24 hours.

For general database use cases, since these users would be performing only read queries against the database, we could add read replicas.

If we had daily pipeline updates, we'd want to be cognizant of any potential downtime. So the updates should be fast. Otherwise we may need to use more complex solutions.
