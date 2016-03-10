#' Query a BigQuery Table
#' 
#' @param projectId The BigQuery project ID
#' @param datasetId A datasetId within projectId
#' @param query BigQuery SQL
#' @param MaxResults Max number per page of results. Set total rows with LIMIT in your query.
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
#' Example: 
#' bqr_query("big-query-r","samples",
#'           "SELECT COUNT(repository.url) FROM [publicdata:samples.github_nested]")
#'           
#' SELECT COUNT(repository.url) as freq, repository.language FROM
#' [publicdata:samples.github_nested]
#' GROUP BY repository.language
#' ORDER BY freq DESC
#' LIMIT
#' 1000
#' 
#' @export
bqr_query <- function(projectId, datasetId, query, maxResults = 1000){
  
  maxResults <- as.numeric(maxResults)
  if(maxResults > 100000) warning("Query not best way to extract large amount of data from BigQuery. Consider creating a table and using bqr_download_data() instead")
  
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
#' Use for big results > 10000 that write to their own destinationTableId
#' 
#' @param projectId projectId to be billed.
#' @param datasetId datasetId of where query will execute.
#' @param query The BigQuery query as a string.
#' @param destinationTableId Id of table the results will be written to.
#' @param writeDisposition Behaviour if destination table exists. See Details.
#' 
#' @details 
#'   writeDisposition - behaviour if destinationTable already exists: 
#'     WRITE_TRUNCATE: BigQuery overwrites the table data. 
#'     WRITE_APPEND: BigQuery appends the data to the table
#'     WRITE_EMPTY: If contains data, a 'duplicate' error is returned
#'     
#' @return A Job object to be queried via \link{bqr_get_job}
#'     
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