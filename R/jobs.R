#' Poll a jobId
#' 
#' @param projectId projectId of job
#' @param jobId jobId to poll
#' 
#' @return A Jobs resource
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