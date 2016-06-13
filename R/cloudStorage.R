#' Update Google Cloud Storage ObjectAccessControls
#' 
#' Requires scopes set in bigQuery.scopes
#'   \code{https://www.googleapis.com/auth/devstorage.full_control}
#'   \code{https://www.googleapis.com/auth/cloud-platform}
#' 
#' @param bucket Google Cloud Storage bucket
#' @param object Object to update
#' @param entity entity to update or add
#' @param entity_type what type of entity
#' @param role Access permission for entity
#' 
#' @seealso \href{https://cloud.google.com/storage/docs/json_api/v1/objectAccessControls/insert}{objectAccessControls on Google API reference}
#' 
#' @examples 
#' 
#' \dontrun{
#' library(bigQueryR)
#' 
#' ## Auth with a project that has at least BigQuery and Google Cloud Storage scope
#' bqr_auth()
#' 
#' ## make a big query
#' job <- bqr_query_asynch("your_project", 
#'                         "your_dataset",
#'                         "SELECT * FROM blah LIMIT 9999999", 
#'                         destinationTableId = "bigResultTable")
#'                         
#' ## poll the job to check its status
#' ## its done when job$status$state == "DONE"
#' bqr_get_job("your_project", job$jobReference$jobId)
#' 
#' ##once done, the query results are in "bigResultTable"
#' ## extract that table to GoogleCloudStorage:
#' # Create a bucket at Google Cloud Storage at 
#' # https://console.cloud.google.com/storage/browser
#' 
#' job_extract <- bqr_extract_data("your_project",
#'                                 "your_dataset",
#'                                 "bigResultTable",
#'                                 "your_cloud_storage_bucket_name")
#'                                 
#' ## poll the extract job to check its status
#' ## its done when job$status$state == "DONE"
#' bqr_get_job("your_project", job_extract$jobReference$jobId)
#' 
#' ## to download via a URL and not logging in via Google Cloud Storage interface:
#' ## Use an email that is Google account enabled
#' ## Requires scopes:
#' ##  https://www.googleapis.com/auth/devstorage.full_control
#' ##  https://www.googleapis.com/auth/cloud-platform
#' ## set via options("bigQueryR.scopes") and reauthenticate if needed
#' 
#' download_url <- bqr_grant_extract_access(job_extract, "your@email.com")
#' 
#' ## download_url may be multiple if the data is > 1GB
#' 
#' }
#' 
#' @family bigQuery upload functions
#' @return TRUE if successful
gcs_update_acl <- function(bucket,
                           object,
                           entity,
                           entity_type = c("user",
                                           "group",
                                           "domian",
                                           "project",
                                           "allUsers",
                                           "allAuthenticatedUsers"),
                           role = c("READER","OWNER")){
  
  entity_type <- match.arg(entity_type)
  role <- match.arg(role)
  
  stopifnot(inherits(bucket, "character"),
            inherits(object, "character"),
            inherits(entity, "character"))
  
  accessControls <- list(
    entity = paste0(entity_type,"-",entity),
    role = role
  )
  
  insert <- 
    googleAuthR::gar_api_generator("https://www.googleapis.com/storage/v1",
                                   "POST",
                                   path_args = list(b = bucket,
                                                    o = object,
                                                    acl = ""))
  
  req <- insert(path_arguments = list(b = bucket, o = object),
                the_body = accessControls)
  
  if(req$status_code == 200){
    myMessage("Access updated")
    out <- TRUE
  } else {
    stop("Error setting access")
  }
  
  out
  
}