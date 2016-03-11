#' bigQueryR
#' 
#' Provides an interface with Google BigQuery
#' 
#' @seealso https://cloud.google.com/bigquery/docs/reference/v2/?hl=en
#' 
#' @docType package
#' @name bigQueryR
NULL

#' Do OAuth2 authentication
#' 
#' @param token An existing OAuth2 token, if you have one.
#' @param new_user Set to TRUE if you want to go through the authentication flow again.
#' 
#' @details 
#' This function just wraps \code{\link[googleAuthR]{gar_auth}} from googleAuthR, 
#'   but means you don't need to explictly load that library.
#'   
#' @seealso \code{\link[googleAuthR]{gar_auth}}
#' 
#' 
#' @export
bqr_auth <- function(token=NULL, new_user=FALSE){
  options("googleAuthR.scopes.selected" = getOption("bigQueryR.scope") )
  options("googleAuthR.client_id" = getOption("bigQueryR.client_id"))
  options("googleAuthR.client_secret" = getOption("bigQueryR.client_secret"))
  options("googleAuthR.webapp.client_id" = getOption("bigQueryR.webapp.client_id"))
  options("googleAuthR.webapp.client_secret" = getOption("bigQueryR.webapp.client_secret"))
  googleAuthR::gar_auth(token=token, new_user=new_user)
}