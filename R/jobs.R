# As job
as.job <- function(x){
  stopifnot(x$kind == "bigquery#job")
  structure(x, class = c("bqr_job", class(x)))
}

# Is job
is.job <- function(x){
  inherits(x, "bqr_job")
}

# metadata only jobs
call_job <- function(projectId, config){
  l <- 
    googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                   "POST",
                                   path_args = list(projects = projectId,
                                                    jobs = ""),
                                   data_parse_function = function(x) x
    )
  
  o <- l(the_body = config)
  as.job(o)
}


#' Wait for a bigQuery job
#' 
#' Wait for a bigQuery job to finish.
#' 
#' @param job A job object 
#' @param wait The number of seconds to wait between checks
#' 
#' Use this function to do a loop to check progress of a job running
#' 
#' @return After a while, a completed job
#' 
#' @family BigQuery asynch query functions  
#' @export
bqr_wait_for_job <- function(job, wait=5){
  
  stopifnot(is.job(job))
  
  status <- FALSE
  time <- Sys.time()
  
  while(!status){
    Sys.sleep(wait)
    myMessage("Waiting for job: ", job$jobReference$jobId, " - Job timer: ", format(difftime(Sys.time(), 
                                                                       time), 
                                                              format = "%H:%M:%S"), level = 3)
    
    job <- bqr_get_job(projectId = job$jobReference$projectId, 
                       jobId = job$jobReference$jobId)
    
    if(getOption("googleAuthR.verbose") <= 2){
      myMessage("job configuration:")
      print(job)
    }
    
    myMessage("Job status: ", job$status$state, level = 3)
    
    if(job$status$state == "DONE"){
      status <- TRUE 
    } else {
      status <- FALSE
    }
  }
  
  if(!is.null(job$status$errorResult)){
    myMessage("Job failed", level = 3)
    warning(job$status$errorResult$message)
    myMessage(job$status$errorResult$message, level = 3)
  }
  
  job
}


#' Poll a jobId
#' 
#' @param projectId projectId of job
#' @param jobId jobId to poll, or a job Object
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
#' bqr_get_job(job$jobReference$jobId, "your_project")
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
#' bqr_get_job(job_extract$jobReference$jobId, "your_project")
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
bqr_get_job <- function(jobId = .Last.value, projectId = bqr_get_global_project()){
  check_bq_auth()
  
  if(is.job(jobId)){
    jobId <- jobId$jobReference$jobId
  }
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
  
  as.job(req$content)
  
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
bqr_list_jobs <- function(projectId = bqr_get_global_project(),
                          allUsers = FALSE,
                          projection = c("full","minimal"),
                          stateFilter = c("done","pending","running")){
  check_bq_auth()
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
  
  lapply(out$jobs, as.job)
  
}
