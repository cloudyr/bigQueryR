context("Authentication")

options(googleAuthR.scopes.selected = "https://www.googleapis.com/auth/cloud-platform")

test_that("Can authenticate", {
  
  Sys.setenv(BQ_AUTH_FILE = "auth.json")
  bqr_auth()
  
  projects <- bqr_list_projects()
  expect_s3_class(projects, "data.frame")
  
})

test_data <- data.frame(Name = c("Season","Test"),
                        Date = as.Date(c("2010-06-30","2010-06-30")),
                        ID = c(1,2),
                        stringsAsFactors = FALSE)

context("Uploads")

test_that("Can upload test set",{
  
  ## canøt query against this too quickly if creating at same runtime
  out <- bqr_upload_data("mark-edmondson-gde", "test2", "test2", test_data)
  
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
  bqr_upload_data(projectId = "mark-edmondson-gde", 
                  datasetId = "test", 
                  tableId = "from_gcs_mtcars", 
                  upload_data = c("gs://bigqueryr-tests/mtcars_test3.csv","gs://bigqueryr-tests/mtcars_test4.csv"),
                  schema = user_schema)
})

context("List tables")

test_that("Can list tables", {
  
  result <- bqr_list_tables("mark-edmondson-gde", "test2")
  expect_equal(result$tableId, "test1")
  
})

context("Query")

test_that("Can query test set", {
  
  result <- bqr_query("mark-edmondson-gde", "test2", "SELECT * FROM [mark-edmondson-gde:test2.test1]")
  
  expect_equal(result$Name, test_data$Name)
  expect_equal(as.Date(result$Date), test_data$Date)
  expect_equal(result$ID, test_data$ID)
  
})

test_that("Single query bug", {
  
  result <- bqr_query("big-query-r","samples",
                      "SELECT repository.url FROM [publicdata:samples.github_nested] LIMIT 10")
  
  ## should be 10, not 1
  expect_equal(nrow(result), 10)
  
})

