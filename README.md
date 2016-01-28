# bigQueryR
R Interface with Google BigQuery.

Very experimental.

For most purposes it would be best if you use [bigrquery](https://github.com/hadley/bigrquery) instead.  Some functions from there are used in this package.

## Why this package then?

This package is here as it uses [googleAuthR](https://github.com/MarkEdmondson1234/googleAuthR) as backend, so has Shiny support, and compatibility with other googleAuthR dependent packages.

An example of a BigQuery Shiny app running OAuth2 is here, the [BigQuery Visualiser](https://mark.shinyapps.io/bigquery-viz/)

## Demo loading Search Console data into BigQuery

```r
library(searchConsoleR)
library(bigQueryR)

## Auth with a project that has at least BigQuery and Search Console scope
googleAuthR::gar_auth()

## get search console daata
sc <- search_analytics("http://www.example.co.uk","2015-12-01","2016-01-28", dimensions = c("date","query"), rowLimit = 5000)

## upload data to BigQuery
## you need to make the dataset (search_console) first
## but the table will get made if not already present
bqr_upload_data("mark-edmondson-gde","search_console","test_wf",sc)

## data now available to query

query <- "SELECT query, count(query) as freq FROM [test.test_wf] 
GROUP BY query
ORDER BY freq DESC
LIMIT 1000"

bqr_query("mark-edmondson-gde","search_console",query)

```
