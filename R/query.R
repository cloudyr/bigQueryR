#' Query a BigQuery Table
#' 
#' @param projectId The BigQuery project ID
#' @param datasetId A datasetId within projectId
#' @param query BigQuery SQL
#' @param maxResults Max number per page of results. Set total rows with LIMIT in your query.
#' 
#' @return a data.frame. 
#'   If there is an SQL error, a data.frame with 
#'   additional class "bigQueryR_query_error" and the 
#'   problem in the data.frame$message
#'   
#' @description 
#'   MaxResults is how many results to return per page of results, which can be less than the 
#' total results you have set in your  query using LIMIT.  Google recommends for bigger datasets
#' to set maxResults = 1000, but this will use more API calls.
#' 
#' @examples 
#' 
#' \dontrun{
#' 
#' bqr_query("big-query-r","samples",
#'           "SELECT COUNT(repository.url) FROM [publicdata:samples.github_nested]")
#' 
#' }
#' 
#' @family BigQuery query functions
#' @export
bqr_query <- function(projectId, datasetId, query, maxResults = 1000){
  
  maxResults <- as.numeric(maxResults)
  if(maxResults > 100000) warning("bqr_query() is not suited to extract large amount of data from BigQuery. Consider using bqr_query_asynch() and bqr_extract_data() instead")
  
  body <- list(
    kind = "bigquery#queryRequest",
    query = query,
    maxResults = maxResults,
    defaultDataset = list(
      datasetId = datasetId,
      projectId = projectId
    )
  )
  
  body <- rmNullObs(body)
  
  q <- googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                      "POST",
                                      path_args = list(projects = projectId,
                                                       queries = ""),
                                      data_parse_function = parse_bqr_query)
  
  data <- try(q(the_body = body,
                path_arguments = list(projects = projectId)))
  
  if(is.error(data)) {
    warning(error.message(data))
    data <- data.frame(error = "SQL Error", message = error.message(data))
    class(data) <- c(class(data), "bigQueryR_query_error")
  }
  
  pageToken <- attr(data, "pageToken")
  if(!is.null(pageToken)){
    message("Paging through query results")
    jobId <- attr(data, "jobReference")$jobId
    pr <- googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                         "GET",
                                         path_args = list(projects = projectId,
                                                          queries = jobId),
                                         pars_args = list(pageToken = pageToken), 
                                         data_parse_function = parse_bqr_query)
    i <- 1
    while(!is.null(pageToken)){
      message("Page #: ", i)
      data_page <- pr(pars_arguments = list(pageToken = pageToken))
      data <- rbind(data, data_page)
      pageToken <- attr(data_page, "pageToken")
      i <- i + 1
    }
    message("All data fetched.")
    
  }
  
  data
  
}


#' BigQuery query asynchronously
#' 
#' Use for big results > 10000 that write to their own destinationTableId.
#' 
#' @param projectId projectId to be billed.
#' @param datasetId datasetId of where query will execute.
#' @param query The BigQuery query as a string.
#' @param destinationTableId Id of table the results will be written to.
#' @param writeDisposition Behaviour if destination table exists. See Details.
#' 
#' @details 
#' 
#' For bigger queries, asynchronous queries save the results to another BigQuery table.  
#' You can check the progress of the job via \link{bqr_get_job}
#' 
#' You may now want to download this data.  
#' For large datasets, this is best done via extracting the BigQuery result to Google Cloud Storage, 
#' then downloading the data from there. 
#' 
#' You can create a bucket at Google Cloud Storage 
#' at \link{https://console.cloud.google.com/storage/browser}
#' 
#' writeDisposition - behaviour if destinationTable already exists: 
#' \itemize{
#'   \item WRITE_TRUNCATE: BigQuery overwrites the table data.
#'   \item WRITE_APPEND: BigQuery appends the data to the table
#'   \item WRITE_EMPTY: If contains data, a 'duplicate' error is returned
#'  }
#'
#'     
#' @return A Job object to be queried via \link{bqr_get_job}
#' 
#' @examples 
#' 
#' \dontrun{
#' library(bigQueryR)
#' 
#' ## Auth with a project that has at least BigQuery and Google Cloud Storage scope
#' bqr_auth()
#' 
#' ## make a big query
#' job <- bqr_query_asynch("your_project", 
#'                         "your_dataset",
#'                         "SELECT * FROM blah LIMIT 9999999", 
#'                         destinationTableId = "bigResultTable")
#'                         
#' ## poll the job to check its status
#' ## its done when job$status$state == "DONE"
#' bqr_get_job("your_project", job$jobReference$jobId)
#' 
#' ##once done, the query results are in "bigResultTable"
#' ## extract that table to GoogleCloudStorage:
#' # Create a bucket at Google Cloud Storage at 
#' # https://console.cloud.google.com/storage/browser
#' 
#' job_extract <- bqr_extract_data("your_project",
#'                                 "your_dataset",
#'                                 "bigResultTable",
#'                                 "your_cloud_storage_bucket_name")
#'                                 
#' ## poll the extract job to check its status
#' ## its done when job$status$state == "DONE"
#' bqr_get_job("your_project", job_extract$jobReference$jobId)
#' 
#' ## to download via a URL and not logging in via Google Cloud Storage interface:
#' ## Use an email that is Google account enabled
#' ## Requires scopes:
#' ##  https://www.googleapis.com/auth/devstorage.full_control
#' ##  https://www.googleapis.com/auth/cloud-platform
#' ## set via options("bigQueryR.scopes") and reauthenticate if needed
#' 
#' download_url <- bqr_grant_extract_access(job_extract, "your@email.com")
#' 
#' ## download_url may be multiple if the data is > 1GB
#' 
#' }
#' 
#'
#' @family BigQuery asynch query functions  
#' @export
bqr_query_asynch <- function(projectId, 
                             datasetId, 
                             query, 
                             destinationTableId,
                             writeDisposition = c("WRITE_EMPTY",
                                                  "WRITE_TRUNCATE",
                                                  "WRITE_APPEND")){
  

  ## make job
  job <- 
    googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                   "POST",
                                   path_args = list(projects = projectId,
                                                    jobs = "")
    )
  
  config <- list(
    jobReference = list(
      projectId = projectId,
      jobId = idempotency() ## uuid to stop duplicates
    ),
    configuration = list(
      query = list(
        allowLargeResults = TRUE,
        defaultDataset = list(
          datasetId = datasetId,
          projectId = projectId
        ),
        destinationTable = list(
          datasetId = datasetId,
          projectId = projectId,
          tableId = destinationTableId
        ),
        query = query,
        writeDisposition = writeDisposition
      )
    )
  )
  
  config <- rmNullObs(config)
  
  req <- job(path_arguments = list(projects = projectId),
             the_body = config)
  
  if(req$status_code == 200){
    myMessage("Query request successful", level=2)
    out <- req$content
  } else {
    stop("Error in query job")
    # out <- FALSE
  }
  
  out
  
}