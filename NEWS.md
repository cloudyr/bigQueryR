# bigQueryR 0.2.0.9000

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

# bigQueryR 0.2.0

* Download asynch queries straight to disk via googleCloudStorageR

# bigQueryR 0.1.0 

* Added a `NEWS.md` file to track changes to the package.
* Initial release
