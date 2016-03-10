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


