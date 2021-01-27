# Data Dictionary

## Author ID
Author ID is an auto-generated primary key column. It has no real meaning outside the schema.

## Author Name
Name of author can be from the Goodreads data if it exists. This can yield more than one author (includes editors, illustrators).

If the  Goodreads data does not exist, then we will try to clean up the raw author data to better match the Goods data.

Examples:

- Janette Brand Miller
- Erin Cabatingan
- Volker R. (Volker Rolf) Berghahn
- Avi
- Selma Lagerlöf
- United States. Maritime Commission

## Avg  Rating
Average Goodreads rating for a book. Will be NULL if we haven't linked a Goodreads book to an SPL bib number.

The rating is a float between 0.0 and 5.0.

## Bib Number
This is a unique identifier used by the Seattle Public Library (SPL) to catalogue their books. This number is not to be confused with the ISBN. Different versions and editions of a book will have different ISBNs. However the SPL will still categorize them under the same Bib Number.

There can be multiple copies of a book under the same Bib Number.

Examples:

- 92742
- 594093
- 1602768
-
## Checkout Date Time
The checkout date time is a timestamp when individual item was checked out. It seems the timestamps are rounded to the minute.

Format: `YYYY-MM-DD HH:MM:SS`
Example: `2020-01-27 15:30:20`

## Item Barcode
The item bar code is the number that distinguishes an individual item in the SPL inventory. This barcode consists of ten digits.

Examples:

- 0010036618121
- 0010036380151
- 0010055782345

## Item Type
The item type contains a code that represents a type of item in the inventory. In this case we only care about print books, so we will be filtering out item types that are not print books.

We don't include this column in the star schema since we're filtered on print books.

Examples:

- acbk
- accd
- jcdvd

## ISBNs
A bib number can contain multiple ISBNs. Each will be listed in a comma separated list.

Example: `1481425730, 1481425749, 9781481425735, 9781481425742`

An ISBN stands for International Standard Book Number. It is used to uniquely identify books.

## Publication Year
The publication year is a clean up version of the publication year since the format used in the SPL inventory is not conducive to analytics queries.

We will use the Goodreads data if possible. If we don't have the Goodreads data, we will try to extract a 4 digit number from the text. If there are multiple years, we will pick the smallest one, assuming it represents the earliest edition.

Examples:
      - 2008
      - 2014
      - 2014
      - 1988
      - 1999

## Publisher ID
Publisher ID is an auto-generated primary key column. It has no real meaning outside the schema.

## Publisher Name
The publisher name is a cleaned up version of the raw Publisher. It should have no duplicates.

The Wikipedia publisher data is preferred when we have a match.

## Ratings Count
Total number of ratings for a Goodreads book. Will be NULL if we haven't linked a Goodreads book to an SPL bib number.

The count is an integer greater than 0.

## Raw Author
Unmodified author of a book in the SPL inventory data.

Examples:

- Brand Miller, Janette, 1952-
- Cabatingan, Erin
- Berghahn, Volker R. (Volker Rolf), 1938-
- Avi, 1937-
- Lagerlöf, Selma, 1858-1940
- United States. Maritime Commission

## Raw Title
Unmodified title of a book in the SPL inventory data.

Examples:

- The Paris pilgrims : a novel / Clancy Carlile.
- Big Bill Haywood and the radical union movement [by] Joseph R. Conlin.
- Japanese arms & armor. Introd. by H. Russell Robinson

## Raw Publisher
Unmodified of publisher of a book in the SPL inventory data.

Examples:

- Schocken : Nextbook,
- Schocken Books
- Schocken

## Raw Publication Year
Unmodified of publication year of a book in the SPL inventory data.

Examples:

      - 2008.
      - [2014]
      - ©2014
      - 1991, c1988.
      - 2003, c1999.
      - 114 So. Washington, Orting 98360-0040)

## Subject ID
Subject ID is an auto-generated primary key column. It has no real meaning outside the schema.

## Subject Name
Subject of book separated from SPL inventory's subjects column. Should be unique.

## Text Reviews Count
Total number of text reviews for a Goodreads book. Will be NULL if we haven't linked a Goodreads book to an SPL bib number.

The count is an integer greater than 0.

## Temperature
Temperature in Fahrenheit for the given day in Seattle. This value is a float.

## Title
The title of the book. The SPL data is messy so it typically contains additional information such as the author, illustrator, and editors.

This column is a cleaned up version of the data. We will try to remove non-title information using some naive heuristics. Also if the Goodreads title is available, we'll use that.
