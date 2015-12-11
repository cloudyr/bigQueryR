#' Do OAuth2 authentication
#' 
#' @param token An existing OAuth2 token, if you have one.
#' @param new_user Set to TRUE if you want to go through the authentication flow again.
#' @param verbose Get more debug messages if set to TRUE
#' 
#' @details 
#' This function just wraps \code{\link[googleAuthR]{gar_auth}} from googleAuthR, 
#'   but means you don't need to explictly load that library.
#'   
#' @seealso \code{\link[googleAuthR]{gar_auth}}
#' 
#' 
#' @export
bqr_auth <- function(token=NULL, new_user=FALSE, verbose = FALSE){
  options("googleAuthR.scopes.selected" = getOption("bigQueryR.scope") )
  options("googleAuthR.client_id" = getOption("bigQueryR.client_id"))
  options("googleAuthR.client_secret" = getOption("bigQueryR.client_secret"))
  options("googleAuthR.webapp.client_id" = getOption("bigQueryR.webapp.client_id"))
  options("googleAuthR.webapp.client_secret" = getOption("bigQueryR.webapp.client_secret"))
  googleAuthR::gar_auth(token=token, new_user=new_user, verbose = verbose)
}

#' List BigQuery datasets
#' 
#' @param projectId The BigQuery project ID
#' 
#' @export
bqr_list_datasets <- function(projectId){
  
  
  l <- googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                      "GET",
                                      path_args = list(projects = projectId,
                                                       datasets = ""),
                                      data_parse_function = function(x) {
                                        if(!is.null(x$datasets)) {
                                          d <- x$datasets
                                          data.frame(datasetId = d$datasetReference$datasetId,
                                                     id = d$id,
                                                     projectId = d$datasetReference$projectId,
                                                     stringsAsFactors = FALSE)
                                        } else {
                                          data.frame(datasetId = "**No Datasets**",
                                                     id = "**No Datasets**",
                                                     projectId = projectId,
                                                     stringsAsFactors = FALSE)
                                        }
                                        })
  l(list(projects = projectId))
  
}

#' List BigQuery tables in a dataset
#' 
#' @param projectId The BigQuery project ID
#' @param datasetId A datasetId within projectId
#' 
#' Example: bqr_list_tables("publicdata", "samples")
#' 
#' @export
bqr_list_tables <- function(projectId, datasetId){
  
  l <- googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                      "GET",
                                      path_args = list(projects = projectId,
                                                       datasets = datasetId,
                                                       tables = ""),
                                      data_parse_function = function(x) {
                                        d <- x$tables
                                        out <- data.frame(id = d$id,
                                                          projectId = d$tableReference$projectId,
                                                          datasetId = d$tableReference$datasetId,
                                                          tableId = d$tableReference$tableId)
                                        
                                        })
  l(path_arguments = list(projects = projectId, 
                          datasets = datasetId))
  
}

#' List Google Dev Console projects you have access to
#' 
#' Example: bqr_list_projects()
#' 
#' @export
bqr_list_projects <- function(){
  
  l <- googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2/projects",
                                      "GET",
                                      data_parse_function = function(x) {
                                        d <- x$projects
                                        out <- data.frame(id = d$id,
                                                          numericId = d$numericId,
                                                          projectId = d$projectReference$projectId,
                                                          friendlyName = d$friendlyName,
                                                          stringsAsFactors = FALSE)
                                        })
  l()
  
}

#' Get BigQuery Table meta data
#' 
#' @param projectId The BigQuery project ID
#' @param datasetId A datasetId within projectId
#' @param tableId The tableId within the datasetId
#' 
#' Example: bqr_table_meta("publicdata", "samples", "github_nested")
#' 
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
#' 
#' Not very useful as can't deal with nested datasets
#' 
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

#' Query a BigQuery Table
#' 
#' @param projectId The BigQuery project ID
#' @param datasetId A datasetId within projectId
#' @param query BigQuery SQL
#' @param MaxResults Max number of results
#' 
#' @return a data.frame. 
#'   If there is an SQL error, a data.frame with 
#'   additional class "bigQueryR_query_error" and the 
#'   problem in the data.frame$message
#' 
#' Example: 
#' bqr_query("big-query-r","samples","github_nested", 
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
  data
  
}


