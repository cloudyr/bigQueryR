## store bucket name
.bqr_env <- new.env(parent = emptyenv())

#' Set global bucket name
#'
#' Set a bucket name used for this R session
#'
#' @param bucket bucket name you want this session to use by default, or a bucket object
#'
#' @details
#'   This sets a bucket to a global environment value so you don't need to
#' supply the bucket argument to other API calls.
#'
#' @return The bucket name (invisibly)
#'
#' @family bucket functions
#' @export
bq_global_project <- function(project){
  
  stopifnot(inherits(project, "character"),
            length(project) == 1)
  
  .bqr_env$project <- project
  message("Set default project to '", project,"'")
  return(invisible(.bqr_env$project))
  
}

#' Get global bucket name
#'
#' Bucket name set this session to use by default
#'
#' @return Bucket name
#'
#' @details
#'   Set the bucket name via \link{gcs_global_bucket}
#'
#' @family bucket functions
#' @export
bq_get_global_project <- function(){
  
  if(!exists("project", envir = .bqr_env)){
    stop("Project is NULL and couldn't find global project ID name.
         Set it via bq_global_project")
  }
  
  .bqr_env$project
  
  }