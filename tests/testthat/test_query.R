## for now assumes local authentication 
context("Do a query")

test_data <- data.frame(Name = c("Season","Test"),
                        Date = as.Date(c("2010-06-30","2010-06-30")),
                        ID = c(1,2),
                        stringsAsFactors = FALSE)

test_that("Can upload test set",{
  
  out <- bqr_upload_data("iih-tools-analytics", "tests", "test1", test_data)
  
  expect_true(out)
  
})

test_that("Can query test set", {
  
  result <- bqr_query("iih-tools-analytics", "tests", "SELECT * FROM test1")
  
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