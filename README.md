# bigQueryR
[![Travis-CI Build Status](https://travis-ci.org/MarkEdmondson1234/bigQueryR.svg?branch=master)](https://travis-ci.org/MarkEdmondson1234/bigQueryR)
[![Analytics](https://ga-beacon.appspot.com/UA-73050356-1/bigQueryR/readme)](https://github.com/MarkEdmondson1234/googleAuthR)

R Interface with Google BigQuery.

Under active development.

For most purposes it would be best if you use [bigrquery](https://github.com/hadley/bigrquery) instead.  Some functions from there are used in this package.

## Why this package then?

This package is here as it uses [googleAuthR](https://github.com/MarkEdmondson1234/googleAuthR) as backend, so has Shiny support, and compatibility with other googleAuthR dependent packages.

It also has support for data extracts, meaning you can download data to Google Cloud Storage and make the download URL available to a user via their Google email. If you do a query normally with over 100000 results it hangs and errors. 

An example of a BigQuery Shiny app running OAuth2 is here, the [BigQuery Visualiser](https://mark.shinyapps.io/bigquery-viz/)

## Demo loading Search Console data into BigQuery

```r
library(searchConsoleR)
library(bigQueryR)

## Auth with a project that has at least BigQuery and Search Console scope
googleAuthR::gar_auth()

## get search console daata
sc <- search_analytics("http://www.example.co.uk",
                       "2015-12-01","2016-01-28", 
                       dimensions = c("date","query"), 
                       rowLimit = 5000)

## upload data to BigQuery
## you need to make the dataset (search_console) first
## but the table will get made if not already present
bqr_upload_data("mark-edmondson-gde","search_console","test_sc",sc)

## data now available to query

query <- "SELECT query, count(query) as freq FROM [search_console.test_sc] 
GROUP BY query
ORDER BY freq DESC
LIMIT 1000"

bqr_query("mark-edmondson-gde","search_console",query)

```

## Demo extracting a big BigQuery > 100000 rows

```r
library(bigQueryR)

## Auth with a project that has at least BigQuery and Google Cloud Storage scope
bqr_auth()

## make a big query
job <- bqr_query_asynch("your_project", 
                        "your_dataset",
                        "SELECT * FROM blah LIMIT 9999999", 
                        destinationTableId = "bigResultTable")

## poll the job to check its status
## its done when job$status$state == "DONE"
bqr_get_job("your_project", job$jobReference$jobId)

## once done, the query results are in "bigResultTable"
## extract that table to GoogleCloudStorage:

# Create a bucket at Google Cloud Storage at 
# https://console.cloud.google.com/storage/browser

job_extract <- bqr_extract_data("your_project",
                                "your_dataset",
                                "bigResultTable",
                                "your_cloud_storage_bucket_name")
                                
## poll the extract job to check its status
## its done when job$status$state == "DONE"
bqr_get_job("your_project", job_extract$jobReference$jobId)

## to download via a URL and not logging in via Google Cloud Storage interface:
## Use an email that is Google account enabled
## Requires scopes:
##  https://www.googleapis.com/auth/devstorage.full_control
##  https://www.googleapis.com/auth/cloud-platform
## set via options("bigQueryR.scopes") and reauthenticate if needed

download_url <- bqr_grant_extract_access(job_extract, "your@email.com")

## download_url may be multiple if the data is > 1GB
> [1] "https://storage.cloud.google.com/big-query-r-extracts/big-query-extract-20160311112410-000000000000.csv"
> [2] "https://storage.cloud.google.com/big-query-r-extracts/big-query-extract-20160311112410-000000000001.csv"
> [3] "https://storage.cloud.google.com/big-query-r-extracts/big-query-extract-20160311112410-000000000002.csv"
```
