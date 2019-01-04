library(googleAuthR)

context("Authentication")

options(googleAuthR.scopes.selected = "https://www.googleapis.com/auth/cloud-platform")

test_that("Can authenticate", {
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  
  projects <- bqr_list_projects()
  expect_s3_class(projects, "data.frame")
  
})

test_that("Set global project", {
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  expect_equal(bq_global_project("mark-edmondson-gde"), 
               "mark-edmondson-gde")
  
})

test_that("Set global dataset", {
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  expect_equal(bq_global_dataset("test2"), 
               "test2")
  
})

test_data <- data.frame(Name = c("Season","Test"),
                        Date = as.Date(c("2010-06-30","2010-06-30")),
                        ID = c(1,2),
                        stringsAsFactors = FALSE)

context("Uploads")

test_that("Can upload test set",{
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  ## canøt query against this too quickly if creating at same runtime
  out <- bqr_upload_data(tableId = "test2", upload_data = test_data)
  
  expect_equal(out$status$state, "DONE")
  
})

test_that("Can upload via Google Cloud Storage",{
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  
  library(googleCloudStorageR)
  gcs_global_bucket("bigqueryr-tests")
  
  f <- function(input, output) {
    write.table(input, sep = ",", 
                col.names = FALSE, 
                row.names = FALSE, 
                quote = FALSE, 
                file = output, 
                qmethod = "double")
  }
  gcs_upload(mtcars, name = "mtcars_test3.csv", object_function = f)
  gcs_upload(mtcars, name = "mtcars_test4.csv", object_function = f)
  
  user_schema <- schema_fields(mtcars)
  bqr_upload_data(datasetId = "test", 
                  tableId = "from_gcs_mtcars", 
                  upload_data = c("gs://bigqueryr-tests/mtcars_test3.csv","gs://bigqueryr-tests/mtcars_test4.csv"),
                  schema = user_schema)
})

test_that("Can upload nested JSON",{
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  
  the_list <- list(list(col1 = "yes", col2 = "no", col3 = list(nest1 = 1, nest2 = 3), col4 = "oh"),
                   list(col1 = "yes2", col2 = "n2o", col3 = list(nest1 = 5, nest2 = 7), col4 = "oh2"), 
                   list(col1 = "yes3", col2 = "no3", col3 = list(nest1 = 7, nest2 = 55), col4 = "oh3"))
  bqr_upload_data(datasetId = "test", 
                  tableId = "nested_list_json", 
                  upload_data = the_list, autodetect = TRUE)
})

context("List tables")

test_that("Can list tables", {
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  
  result <- bqr_list_tables()
  expect_true("test1" %in% result$tableId)
  
})

context("Query")

test_that("Can query test set", {
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  result <- bqr_query(query = "SELECT * FROM test1")
  
  expect_equal(result$Name, test_data$Name)
  expect_equal(as.Date(result$Date), test_data$Date)
  expect_equal(result$ID, test_data$ID)
  
})

test_that("Single query bug", {
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  
  result <- bqr_query(query = "SELECT repository.url FROM [publicdata:samples.github_nested] LIMIT 10")
  
  ## should be 10, not 1
  expect_equal(nrow(result), 10)
  
})

test_that("Async query", {
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  
  job <- bqr_query_asynch(query = "SELECT * FROM test1", 
                             destinationTableId = "test3", 
                             writeDisposition = "WRITE_TRUNCATE")
  
  expect_equal(job$kind, "bigquery#job")
  
  job <- bqr_wait_for_job(job)
  expect_equal(job$status$state, "DONE")
  expect_null(job$status$errorResult)
  
})

context("Downloading extracts")

test_that("Extract data to Google Cloud Storage, and download", {
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  
  gcs_global_bucket("bigqueryr-tests")
  job_extract <- bqr_extract_data(tableId = "test3",
                                  cloudStorageBucket = gcs_get_global_bucket())
  
  expect_equal(job_extract$kind, "bigquery#job")
  expect_null(job_extract$status$errorResult)
  
  job <- bqr_wait_for_job(job_extract)
  
  expect_equal(job$status$state, "DONE")
  
  urls <- bqr_grant_extract_access(job, email = "m@sunholo.com")
  expect_true(grepl("https://storage.cloud.google.com/bigqueryr-tests/big-query-extract", urls))
  
  extract <- bqr_download_extract(job)
  file_name <- basename(job$configuration$extract$destinationUri)
  
  downloaded <- list.files(pattern = gsub("-\\*\\.csv","", file_name))
  expect_true(file.exists(downloaded))
  unlink(downloaded)
})

context("Tables")

test_that("Create a table", {
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  
  table <- bqr_create_table(tableId = "created_table", template_data = mtcars)
  
  expect_true(table)
  
})

test_that("Get meta data of table", {
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  
  meta <- bqr_table_meta(tableId = "created_table")
  
  expect_equal(meta$kind, "bigquery#table")
  
})

test_that("Get data of table", {
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  
  meta <- bqr_table_data(tableId = "created_table")
  
  expect_equal(meta$kind, "bigquery#tableDataList")
  
})

test_that("Delete a table", {
  skip_on_cran()
  skip_if_no_env_auth("BQ_AUTH_FILE")
  
  table <- bqr_delete_table(tableId = "created_table")
  
  expect_true(table)
  
})
