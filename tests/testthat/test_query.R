## for now assumes local authentication 
context("Do a query")

test_data <- data.frame(Name = c("Season","Test"),
                        Date = as.Date(c("2010-06-30","2010-06-30")),
                        ID = c(1,2),
                        stringsAsFactors = FALSE)

test_that("Can upload test set",{
  
  out <- bqr_upload_data("mark-edmondson-gde", "test2", "test1", test_data)
  
  expect_true(out)
  
})

test_that("Can query test set", {
  
  result <- bqr_query("mark-edmondson-gde", "test2", "SELECT * FROM test1")
  
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

test_that("Can upload via Google Cloud Storage",{
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
  bqr_upload_data(projectId = "mark-edmondson-gde", 
                  datasetId = "test", 
                  tableId = "from_gcs_mtcars", 
                  upload_data = c("gs://bigqueryr-tests/mtcars_test3.csv","gs://bigqueryr-tests/mtcars_test4.csv"),
                  schema = user_schema)
})