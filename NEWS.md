# bigQueryR 0.5.0

* Support listing more than 50 datasets in `bqr_list_dataset`
* Change `bqr_list_tables` to list all tables in a dataset by default
* Add `bqr_copy_dataset`
* Add `Table` and `bqr_update_table`
* Support uploading nested lists via `toJSON`
* Add writeDisposition to table loads
* Allow creation of empty tables
* Supporting supplying SQL via a file ending with `.sql` for `bqr_query()`
* Update to new `googleAuthR>1.1.1`

# bigQueryR 0.4.0

* support `nullMarker`, `maxBadRecords`, `fieldDelimiter` in upload jobs
* Support BigQuery type `DATE` for R class `Date` data.frame columns (BigQuery type `TIMESTAMP` still default for `POSIXct`columns) (#48)
* Allow custom user schema for uploads of data.frames (#48)
* Rename misnamed global functions from `bq_` prefix to `bqr_` prefix
* Add `allowJaggedRows` and `allowQuotedNewlines` options to upload via `bqr_upload_data()`
* `bqr_get_job` now accepts a job object as well as the jobId
* Fix bug with `bqr_upload_data` where `autodetect=TRUE` didn't work with `gcs://` loads from Cloud Storage
* Fix bug with `bqr_query()` that caused a 404 error sometimes. 

# bigQueryR 0.3.2

* Move to new batch endpoint (#41)


# bigQueryR 0.3.1

* Fix asynch job fail if user previously `set.seed()` (#37)
* skip tests on CRAN causing error
* fix warning in scope check (#40)

# bigQueryR 0.3.0

* Add support for realtime queries, `useQueryCache = FALSE`
* Add support for standard SQL (#21)
* Add support for hms/timestamp class uploads (#27)
* Add support for partitioned tables (#28)
* Fix bug that only returned one row for single column queries (#31 - thanks Rob)
* Allow loading of data from Google Cloud Storage to BigQuery for large files
* no error if delete non-existent table (#26)
* Add auto authentication if set environment var `BQ_AUTH_FILE` to location of auth file
* Add default project if set environment var `BQ_DEFAULT_PROJECT_ID` to project-id
* Add default dataset if set environment var `BQ_DEFAULT_DATASET` to dataset-id
* Add auto paging through table lists in `bqr_list_tables()` (#29)
* Make it clearer when jobs resulted in errors in the job print methods
* Migrate to using `googleCloudStorageR` for Cloud Storage stuff
* Set default authentication scope to `https://www.googleapis.com/auth/cloud-platform`
* Unit tests
* Upload table will now correctly report errors
* More user feedback on BigQuery jobs when running
* Allow upload of data.frames asynchrnously
* Allow auto-detection of schema for uploads

# bigQueryR 0.2.0

* Download asynch queries straight to disk via googleCloudStorageR

# bigQueryR 0.1.0 

* Added a `NEWS.md` file to track changes to the package.
* Initial release
