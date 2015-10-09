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
                                      data_parse_function = function(x) x$datasets)
  l(projectId)
  
}