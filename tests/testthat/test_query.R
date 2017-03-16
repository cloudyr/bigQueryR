context("Authentication")

options(googleAuthR.scopes.selected = "https://www.googleapis.com/auth/cloud-platform")

test_that("Can authenticate", {
  
  Sys.setenv(BQ_AUTH_FILE = "auth.json")
  bqr_auth()
  
  projects <- bqr_list_projects()
  expect_s3_class(projects, "data.frame")
  
})

test_that("Set global project", {
  
  expect_equal(bq_global_project("mark-edmondson-gde"), 
               "mark-edmondson-gde")
  
})

test_that("Set global dataset", {
  
  expect_equal(bq_global_dataset("test2"), 
               "test2")
  
})

test_data <- data.frame(Name = c("Season","Test"),
                        Date = as.Date(c("2010-06-30","2010-06-30")),
                        ID = c(1,2),
                        stringsAsFactors = FALSE)

context("Uploads")

test_that("Can upload test set",{
  
  ## canøt query against this too quickly if creating at same runtime
  out <- bqr_upload_data(tableId = "test2", upload_data = test_data)
  
  expect_true(out)
  
})

test_that("Can upload via Google Cloud Storage",{
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

context("List tables")

test_that("Can list tables", {
  
  result <- bqr_list_tables()
  expect_true("test1" %in% result$tableId)
  
})

context("Query")

test_that("Can query test set", {
  
  result <- bqr_query(query = "SELECT * FROM test1")
  
  expect_equal(result$Name, test_data$Name)
  expect_equal(as.Date(result$Date), test_data$Date)
  expect_equal(result$ID, test_data$ID)
  
})

test_that("Single query bug", {
  
  result <- bqr_query(query = "SELECT repository.url FROM [publicdata:samples.github_nested] LIMIT 10")
  
  ## should be 10, not 1
  expect_equal(nrow(result), 10)
  
})

test_that("Async query", {
  
  result <- bqr_query_asynch(query = "SELECT * FROM test1", 
                             destinationTableId = "test3", 
                             writeDisposition = "WRITE_TRUNCATE")
  
  expect_equal(result$kind, "bigquery#job")
  
  job <- bqr_wait_for_job(result)
  expect_equal(job$status$state, "DONE")
  expect_null(job$status$errorResult)
  
})

context("Downloading extracts")

test_that("Extract data to Google Cloud Storage", {
  
  gcs_global_bucket("bigqueryr-tests")
  job_extract <- bqr_extract_data(tableId = "test3",
                                  cloudStorageBucket = gcs_get_global_bucket())
  
  expect_equal(job_extract$kind, "bigquery#job")
  expect_null(job_extract$status$errorResult)
  
  job <- bqr_wait_for_job(job_extract)
  
  expect_equal(job$status$state, "DONE")
  
  urls <- bqr_grant_extract_access(job, email = "m@sunholo.com")
  expect_true(grepl("https://storage.cloud.google.com/bigqueryr-tests/big-query-extract", urls))
                                 
})
