#' Query a BigQuery Table
#' 
#' @param projectId The BigQuery project ID
#' @param datasetId A datasetId within projectId
#' @param query BigQuery SQL.  You can also supply a file location of your query ending with \code{.sql}
#' @param maxResults Max number per page of results. Set total rows with LIMIT in your query.
#' @param useLegacySql Whether the query you pass is legacy SQL or not. Default TRUE
#' @param useQueryCache Whether to use the query cache. Default TRUE, set to FALSE for realtime queries. 
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
#' @seealso \href{https://cloud.google.com/bigquery/sql-reference/}{BigQuery SQL reference}
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
bqr_query <- function(projectId = bqr_get_global_project(), 
                      datasetId = bqr_get_global_dataset(), 
                      query, 
                      maxResults = 1000, 
                      useLegacySql = TRUE, 
                      useQueryCache = TRUE,
                      dryRun = FALSE,
                      timeoutMs = 600*1000){
  check_bq_auth()
  
  if(endsWith(query, ".sql")){
    query <- readChar(query, nchars = file.info(query)$size)
  }
  
  maxResults <- as.numeric(maxResults)
  if(maxResults > 100000) warning("bqr_query() is not suited to extract large amount of data from BigQuery. Consider using bqr_query_asynch() and bqr_extract_data() instead")
  
  body <- list(
    kind = "bigquery#queryRequest",
    query = query,
    maxResults = maxResults,
    useLegacySql = useLegacySql,
    useQueryCache = useQueryCache,
    defaultDataset = list(
      datasetId = datasetId,
      projectId = projectId
    ),
    timeoutMs = timeoutMs,
    dryRun = dryRun
  )
  
  body <- rmNullObs(body)
  
  # solve 404?
  the_url <- sprintf("https://www.googleapis.com/bigquery/v2/projects/%s/queries", projectId)
  
  if(dryRun){
    q <- googleAuthR::gar_api_generator(the_url,
                                        "POST",
                                        checkTrailingSlash = FALSE)
    data <- try(q(the_body = body,
                  path_arguments = list(projects = projectId)))
    if(!is.error(data)){
      data <- data$content  
    }
    
  }else{
    q <- googleAuthR::gar_api_generator(the_url,
                                        "POST",
                                        data_parse_function = parse_bqr_query,
                                        checkTrailingSlash = FALSE)
    data <- try(q(the_body = body,
                  path_arguments = list(projects = projectId)))  
  }
  
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
#' @param useLegacySql Whether the query you pass is legacy SQL or not. Default TRUE
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
#' You can read how to create a bucket at Google Cloud Storage 
#' at \url{https://cloud.google.com/storage/docs/cloud-console}
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
#' bqr_get_job(job$jobReference$jobId, "your_project")
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
#' bqr_get_job(job_extract$jobReference$jobId, "your_project")
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
bqr_query_asynch <- function(projectId = bqr_get_global_project(), 
                             datasetId = bqr_get_global_dataset(), 
                             query, 
                             destinationTableId,
                             useLegacySql = TRUE,
                             writeDisposition = c("WRITE_EMPTY",
                                                  "WRITE_TRUNCATE",
                                                  "WRITE_APPEND")){
  
  writeDisposition <- match.arg(writeDisposition)
  
  check_bq_auth()
  ## make job
  job <- 
    googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                   "POST",
                                   path_args = list(projects = projectId,
                                                    jobs = "")
    )
  
  config <- list(
    jobReference = list(
      projectId = projectId
     ## jobId = idempotency() ## uuid to stop duplicates - breaks if set.seed() (#37)
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
        useLegacySql = useLegacySql,
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
  
  as.job(out)
  
}
