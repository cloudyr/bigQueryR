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
#' @importFrom googleAuthR gar_api_generator gar_api_page
#' @export
bqr_list_datasets <- function(projectId = bqr_get_global_project()){
  
  check_bq_auth()
  l <- gar_api_generator("https://www.googleapis.com/bigquery/v2",
                         "GET",
                         path_args = list(projects = projectId,
                                          datasets = ""),
                         pars_args = list(pageToken=""),
                         data_parse_function = parse_list_datasets)
  pages <- gar_api_page(l, 
                        page_f = get_attr_nextpagetoken,
                        page_method = "param",
                        page_arg = "pageToken")
  
  Reduce(rbind, pages)
  
}

#' @import assertthat
#' @noRd
parse_list_datasets <- function(x){

  assert_that(x$kind == "bigquery#datasetList")

    if(!is.null(x$datasets)) {
      d <- x$datasets
      o <- data.frame(datasetId = d$datasetReference$datasetId,
                 id = d$id,
                 projectId = d$datasetReference$projectId,
                 location = d$location,
                 stringsAsFactors = FALSE)
    } else {
      o <- data.frame()
    }
  attr(o, "nextPageToken") <- x$nextPageToken
  o
}


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


