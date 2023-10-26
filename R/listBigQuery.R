#' List Google Dev Console projects you have access to
#' 
#' Example: bqr_list_projects()
#' 
#' @return A dataframe of the projects you have access to under the authentication
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
  check_bq_auth()
  l <- googleAuthR::gar_api_generator("https://bigquery.googleapis.com/bigquery/v2/projects",
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


