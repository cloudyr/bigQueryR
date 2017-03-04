#' List BigQuery datasets
#' 
#' Each projectId can have multiple datasets.
#' 
#' @param projectId The BigQuery project ID
#' 
#' @examples 
#' 
#' \dontrun{
#'   library(bigQueryR)
#'   
#'   ## this will open your browser
#'   ## Authenticate with an email that has access to the BigQuery project you need
#'   bqr_auth()
#'   
#'   ## verify under a new user
#'   bqr_auth(new_user=TRUE)
#'   
#'   ## get projects
#'   projects <- bqr_list_projects()
#'   
#'   my_project <- projects[1]
#'   
#'   ## for first project, get datasets
#'   datasets <- bqr_list_datasets[my_project]
#'   
#' }
#' 
#' @family bigQuery meta functions
#' @export
bqr_list_datasets <- function(projectId = bq_get_global_project()){
  
  
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


#' List Google Dev Console projects you have access to
#' 
#' Example: bqr_list_projects()
#' 
#' @return A dataframe of the projects you have access to under the authenitcation
#' 
#' @examples 
#' 
#' \dontrun{
#'   library(bigQueryR)
#'   
#'   ## this will open your browser
#'   ## Authenticate with an email that has access to the BigQuery project you need
#'   bqr_auth()
#'   
#'   ## verify under a new user
#'   bqr_auth(new_user=TRUE)
#'   
#'   ## get projects
#'   projects <- bqr_list_projects()
#'   
#' }
#' 
#' @family bigQuery meta functions
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


