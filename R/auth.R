# check authenticated with correct scopes
check_bq_auth <- function(){
  cloud_scopes <- c("https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/bigquery")
  
  if(!any(getOption("googleAuthR.scopes.selected") %in% cloud_scopes)){
    stop("Scopes not adequete for Google BigQuery.  Needs to be one of ", 
         paste(cloud_scopes, collapse = " "))
    googleAuthR::gar_token_info(2)

  }
}

# check authenticated with correct scopes
check_gcs_auth <- function(){
  cloud_scopes <- c("https://www.googleapis.com/auth/cloud-platform", 
                    "https://www.googleapis.com/auth/devstorage.full_control",
                    "https://www.googleapis.com/auth/devstorage.read_write")
  
  if(!any(getOption("googleAuthR.scopes.selected") %in% cloud_scopes)){
    stop("Not authenticated with Google Cloud Storage.  Needs to be one of ", 
         paste(cloud_scopes, collapse = " "))
    current_op <- getOption("googleAuthR.verbose")
    options(googleAuthR.verbose = 2)
    googleAuthR::gar_token_info()
    options(googleAuthR.verbose = current_op)
  }
}

#' Authenticate this session
#'
#' Autheticate manually via email or service JSON file
#' 
#' @param json_file Authentication json file you have downloaded from your Google Project
#' @param token A preexisting token to authenticate with
#' @param email A Google email to authenticate with
#'
#' If you have set the environment variable \code{BQ_AUTH_FILE} to a valid file location,
#'   the function will look there for authentication details.
#' Otherwise it will trigger an authentication flow via Google login screen in your browser based on the email you provide.
#'
#' If \code{BQ_AUTH_FILE} is specified, then authentication will be called upon loading the package
#'   via \code{library(bigQueryR)},
#'   meaning that calling this function yourself at the start of the session won't be necessary.
#'
#' \code{BQ_AUTH_FILE} is a GCP service account JSON ending with file extension \code{.json}
#'
#' @return Invisibly, the token that has been saved to the session
#' @importFrom googleAuthR gar_auth  gar_auth_service
#' @importFrom tools file_ext
#' @export
#' @examples 
#' 
#' \dontrun{
#' 
#' # to use default package credentials (for testing)
#' library(bigQueryR)
#' bqr_auth("location_of_json_file.json")
#' 
#' # or via email
#' bqr_auth(email="me@work.com")
#' 
#' # to use your own Google Cloud Project credentials
#' # go to GCP console and download client credentials JSON 
#' # ideally set this in .Renviron file, not here but just for demonstration
#' Sys.setenv("GAR_CLIENT_JSON" = "location/of/file.json")
#' library(bigQueryR)
#' # should now be able to log in via your own GCP project
#' bqr_auth()
#' 
#' # reauthentication
#' # Once you have authenticated, set email to skip the interactive message
#' bqr_auth(email = "my@email.com")
#' 
#' # or leave unset to bring up menu on which email to auth with
#' bqr_auth()
#' # The bigQueryR package is requesting access to your Google account. 
#' # Select a pre-authorised account or enter '0' to obtain a new token.
#' # Press Esc/Ctrl + C to abort.
#' #1: my@email.com
#' #2: work@mybusiness.com

#' # you can set authentication for many emails, then switch between them e.g.
#' bqr_auth(email = "my@email.com")
#' bqr_list_projects() # lists what GCP projects you have access to
#' bqr_auth(email = "work@mybusiness.com") 
#' bqr_list_projects() # lists second set of projects
#' 
#' 
#' 
#' }
bqr_auth <- function(json_file = NULL,
                     token = NULL, 
                     email = Sys.getenv("GARGLE_EMAIL")){
  
  set_scopes()
  
  if(is.null(json_file)){
    gar_auth(token = token,
             email = email,
             package = "bigQueryR")    
  } else {
    gar_auth_service(json_file = json_file)
  }

}

set_scopes <- function(){
  required_scopes <- c("https://www.googleapis.com/auth/bigquery",
                       "https://www.googleapis.com/auth/cloud-platform")
  
  op <- getOption("googleAuthR.scopes.selected")
  if(is.null(op)){
    options(googleAuthR.scopes.selected = "https://www.googleapis.com/auth/bigquery")
  } else if(!any(op %in% required_scopes)){
    myMessage("Adding https://www.googleapis.com/auth/bigquery to scopes", level = 3)
    options(googleAuthR.scopes.selected = c(op, "https://www.googleapis.com/auth/bigquery"))
  }
}