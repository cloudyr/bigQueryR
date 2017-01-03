#' List BigQuery tables in a dataset
#' 
#' @param projectId The BigQuery project ID
#' @param datasetId A datasetId within projectId
#' @param maxResults Number of results to return, default \code{1000}
#' @param pageToken The tableID to start listing from, for more than 1000 result paging
#' 
#' @return dataframe of tables in dataset
#' 
#' @examples 
#' 
#' \dontrun{
#'  bqr_list_tables("publicdata", "samples")
#' }
#' 
#' @family bigQuery meta functions
#' @export
bqr_list_tables <- function(projectId, datasetId, maxResults = 1000, pageToken = ""){
  
  l <- googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                      "GET",
                                      path_args = list(projects = projectId,
                                                       datasets = datasetId,
                                                       tables = ""),
                                      pars_args = list(maxResults = maxResults,
                                                       pageToken = pageToken))
  
  out <- l(path_arguments = list(projects = projectId, 
                                 datasets = datasetId))
  
  # if(!is.null(out$content$nextPageToken)){
  #   npt <- out$content$nextPageToken
  #   myMessage("Paging through results: ", npt, level = 2)
  # }
  
  out
}

parse_bqr_list_tables <- function(x) {
  d <- x$tables
  data.frame(id = d$id,
             projectId = d$tableReference$projectId,
             datasetId = d$tableReference$datasetId,
             tableId = d$tableReference$tableId, stringsAsFactors = FALSE)
  
}

#' Get BigQuery Table meta data
#' 
#' @param projectId The BigQuery project ID
#' @param datasetId A datasetId within projectId
#' @param tableId The tableId within the datasetId
#' 
#' @return list of table metadata
#' 
#' @examples 
#' 
#' \dontrun{
#'   bqr_table_meta("publicdata", "samples", "github_nested")
#' }
#' 
#' 
#' @family bigQuery meta functions
#' @export
bqr_table_meta <- function(projectId, datasetId, tableId){
  
  
  f <- function(x){
    x <- rmNullObs(x)
  }
  
  
  l <- googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                      "GET",
                                      path_args = list(projects = projectId,
                                                       datasets = datasetId,
                                                       tables = tableId),
                                      data_parse_function = f)
  
  l(path_arguments = list(projects = projectId, 
                          datasets = datasetId, 
                          tables = tableId))
  
}

#' Get BigQuery Table's data list
#' 
#' @param projectId The BigQuery project ID
#' @param datasetId A datasetId within projectId
#' @param tableId The tableId within the datasetId
#' @param maxResults Number of results to return
#' 
#' @return data.frame of table data
#' 
#' This won't work with nested datasets, for that use \link{bqr_query} as that flattens results.
#' 
#' @family bigQuery meta functions
#' @export
bqr_table_data <- function(projectId, datasetId, tableId,
                           maxResults = 1000){
  
  l <- googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                      "GET",
                                      path_args = list(projects = projectId,
                                                       datasets = datasetId,
                                                       tables = tableId,
                                                       data = ""),
                                      pars_args = list(maxResults = maxResults),
                                      data_parse_function = function(x) x)
  
  l(path_arguments = list(projects = projectId, 
                          datasets = datasetId, 
                          tables = tableId),
    pars_arguments = list(maxResults = maxResults))
  
}


#' Create a Table
#' 
#' @param projectId The BigQuery project ID.
#' @param datasetId A datasetId within projectId.
#' @param tableId Name of table you want.
#' @param template_data A dataframe with the correct types of data
#' @param timePartitioning Whether to create a partioned table
#' @param expirationMs If a partioned table, whether to have an expiration time on the data. The default \code{0} is no expiration.
#' 
#' @return TRUE if created, FALSE if not.  
#' 
#' @details 
#' 
#' Creates a BigQuery table.
#' 
#' If setting \code{timePartioning} to \code{TRUE} then the table will be a 
#'   \href{partioned table}{https://cloud.google.com/bigquery/docs/creating-partitioned-tables}
#' 
#' @family bigQuery meta functions
#' @export
bqr_create_table <- function(projectId, 
                             datasetId, 
                             tableId, 
                             template_data,
                             timePartitioning = FALSE,
                             expirationMs = 0L){
  
  l <- googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                      "POST",
                                      path_args = list(projects = projectId,
                                                       datasets = datasetId,
                                                       tables = "")
                                      )
  expirationMs <- as.integer(expirationMs)
  timeP <- NULL
  if(timePartitioning){
    if(expirationMs == 0) expirationMs <- NULL
    timeP <- list(type = "DAY", expirationMs = expirationMs)
  }
  
  config <- list(
        schema = list(
          fields = schema_fields(template_data)
        ),
        tableReference = list(
          projectId = projectId,
          datasetId = datasetId,
          tableId = tableId
        ),
        timePartitioning = timeP
  )
  
  config <- rmNullObs(config)
  
  req <- try(l(path_arguments = list(projects = projectId, 
                                     datasets = datasetId),
               the_body = config), silent = TRUE)
  
  if(is.error(req)){
    if(grepl("Already Exists", error.message(req))){
      message("Table exists: ", tableId, "Returning FALSE")
      out <- FALSE
    } else {
      stop(error.message(req))
    }
  } else {
    message("Table created: ", tableId)
    out <- TRUE
  }
  
  out
  
}


#' Delete a Table
#' 
#' @param projectId The BigQuery project ID.
#' @param datasetId A datasetId within projectId.
#' @param tableId Name of table you want to delete.
#' 
#' @return TRUE if deleted, error if not.  
#' 
#' @details 
#' 
#' Deletes a BigQuery table
#' 
#' @family bigQuery meta functions
#' @export
bqr_delete_table <- function(projectId, datasetId, tableId){
  
  l <- googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                      "DELETE",
                                      path_args = list(projects = projectId,
                                                       datasets = datasetId,
                                                       tables = tableId)
  )
  
  l(path_arguments = list(projects = projectId, 
                          datasets = datasetId,
                          tables = tableId))
  
  TRUE
  
}