# Introduction

Note that these queries go along with an [ebook](https://www.doit.com/resources/the-bigquery-optimization-handbook-preparing-to-save/) about optimizing BigQuery
costs written by Sayle Matthews of DoiT International.

These queries are to assist in optimizing BigQuery usage in projects and organizations.
Most focus on costs, but there are a few that focus on concurrency and also some that
recommend whether a query will run better under an on-demand or flat-rate pricing scheme.

## Usage

In each file is a variable called interval_in_days which is the number of days in the past
that the query will look at for doing its work. In order to increase or decrease the amount of
data processed by the query just change this value and it will be reflected throughout the 
rest of the query.

Note that we have set this value to a default that reflects a good timeframe vs amount of
data processed by it.

Additionally each file should be named on what exactly it does and also has a comment at
the top of explaining what the query does.

## Note on Costs

Some of these queries can process through a LOT of data so it's HIGHLY recommended to
verify the estimated cost of each query before running it. Depending upon how much
usage your dataset sees over the specified timeframe then this could easily be upwards
of tens of gigabytes if not more per query.

Blindly run these queries at your own risk! It's very much recommended to reduce the
interval_in_days value when the query will be processing a very large amount of data.

## Audit Log vs Information Schema

The queries are broken up into two different folders: audit_log and information_schema.
These correspond to the different schemas that may need to be queried. In general most
people will use the information_schema queries because they do not have a BigQuery audit
log sink setup.

If you have an audit log sink setup for BigQuery already (or are a DoiT customer with the
BQ Lens feature enabled) read the blog entry for a detailed guide of how to discover the
location of your sink.

Note that if you are currently a DoiT International customer and have the BQ Lens feature enabled
in your Cloud Management Platform (CMP) then you should use the audit log queries as you will
already have the tables created

## Generating Queries for Your Project

If you look at the queries you will see some placeholders for <project-name>
and <dataset-region> in the SQL code. These need to be replaced with the
correct values prior to running the queries.

In order to assist in doing this easier there is a file called generate_sql_files.py
inside of this repository. This will perform a search and replace operation on all of
the .sql files with your specified values.

## generate_sql_files.py Usage

```bash
generate_sql_files.py [--location <dataset-location>] <project> <output-directory>
```

Note that this will create an exact copy of the files in the directory you specify
as the output-directory.

location:  
This is the location of your BigQuery dataset. Note the format is the same as BigQuery's such
as 'region-us' for the US multi-region or 'us-central1' for the US Central Zone 1 region. The
default value is 'region-us' if you do not specify anything.

project:  
This is the name of the GCP project you are going to be querying against.

output-directory:  
The directory where you would like the generated files to be stored. The script will
attempt to create this directory if it doesn't already exist. Note that this should not
be the same directory as the one where the script is located.

## Contributing
If you see any bugs please feel free to reach out or perform a pull request on the code.
