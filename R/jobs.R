#' Poll a jobId
#' 
#' @param projectId projectId of job
#' @param jobId jobId to poll
#' 
#' @return A Jobs resource
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
#' 
#' 
#' 
#' @family BigQuery asynch query functions  
#' @export
bqr_get_job <- function(projectId, jobId){
  
  stopifnot(inherits(projectId, "character"),
            inherits(jobId, "character"))
  
  ## make job
  job <- 
    googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                   "GET",
                                   path_args = list(projects = projectId,
                                                    jobs = jobId))
  
  req <- job(path_arguments = list(projects = projectId,
                                   jobs = jobId))
  
  req$content
  
}

#' List BigQuery jobs
#' 
#' @description 
#'   List the BigQuery jobs for the projectId
#' 
#' @details 
#' Lists all jobs that you started in the specified project. 
#' Job information is available for a six month period after creation. 
#' The job list is sorted in reverse chronological order, by job creation time. 
#' Requires the Can View project role, or the 
#'   Is Owner project role if you set the allUsers property.
#' 
#' @param projectId projectId of job
#' @param allUsers Whether to display jobs owned by all users in the project.
#' @param projection "full" - all job data, "minimal" excludes job configuration.
#' @param stateFilter Filter for job status.
#' 
#' @return A list of jobs resources
#' @export
bqr_list_jobs <- function(projectId,
                          allUsers = FALSE,
                          projection = c("full","minimal"),
                          stateFilter = c("done","pending","running")){
  
  stopifnot(inherits(projectId, "character"),
            inherits(allUsers, "logical"))
  
  projection <- match.arg(projection)
  stateFilter <- match.arg(stateFilter)
  
  pars <- list(allUsers = allUsers,
               projection = projection,
               stateFilter = stateFilter)
  
  options("googleAuthR.jsonlite.simplifyVector" = FALSE )
  ## make job
  job <- 
    googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                   "GET",
                                   path_args = list(projects = projectId,
                                                    jobs = ""),
                                   pars_args = pars)
  req <- job(path_arguments = list(projects = projectId),
             pars_argumenets = pars)
  
  out <- rmNullObs(req$content)
  options("googleAuthR.jsonlite.simplifyVector" = TRUE )
  
  out
  
}