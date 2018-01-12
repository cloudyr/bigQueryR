## store project name
.bqr_env <- new.env(parent = emptyenv())

#' Set global project name
#'
#' Set a project name used for this R session
#'
#' @param project project name you want this session to use by default, or a project object
#'
#' @details
#'   This sets a project to a global environment value so you don't need to
#' supply the project argument to other API calls.
#'
#' @return The project name (invisibly)
#'
#' @family project functions
#' @export
bq_global_project <- function(project){
  .Deprecated("bqr_global_project")
  stopifnot(inherits(project, "character"),
            length(project) == 1)
  
  .bqr_env$project <- project
  message("Set default project to '", project,"'")
  return(invisible(.bqr_env$project))
  
}

#' @rdname bq_global_project
#' @export
bqr_global_project <- bq_global_project

#' Get global project name
#'
#' project name set this session to use by default
#'
#' @return project name
#'
#' @details
#'   Set the project name via \link{bq_global_project}
#'
#' @family project functions
#' @export
bq_get_global_project <- function(){
  .Deprecated("bqr_get_global_project")
  if(!exists("project", envir = .bqr_env)){
    stop("Project is NULL and couldn't find global project ID name.
         Set it via bq_global_project")
  }
  
  .bqr_env$project
  
}

#' @rdname bq_get_global_project
#' @export
bqr_get_global_project <- bq_get_global_project

#' Set global dataset name
#'
#' Set a dataset name used for this R session
#'
#' @param dataset dataset name you want this session to use by default, or a dataset object
#'
#' @details
#'   This sets a dataset to a global environment value so you don't need to
#' supply the dataset argument to other API calls.
#'
#' @return The dataset name (invisibly)
#'
#' @family dataset functions
#' @export
bq_global_dataset <- function(dataset){
  .Deprecated("bqr_global_dataset")
  stopifnot(inherits(dataset, "character"),
            length(dataset) == 1)
  
  .bqr_env$dataset <- dataset
  message("Set default dataset to '", dataset,"'")
  return(invisible(.bqr_env$dataset))
  
}

#' @rdname bq_global_dataset
#' @export
bqr_global_dataset <- bq_global_dataset

#' Get global dataset name
#'
#' dataset name set this session to use by default
#'
#' @return dataset name
#'
#' @details
#'   Set the dataset name via \link{bq_global_dataset}
#'
#' @family dataset functions
#' @export
bq_get_global_dataset <- function(){
  .Deprecated("bqr_get_global_dataset")
  if(!exists("dataset", envir = .bqr_env)){
    stop("dataset is NULL and couldn't find global dataset ID name.
         Set it via bq_global_dataset")
  }
  
  .bqr_env$dataset
  
}

#' @rdname bq_get_global_dataset
#' @export
bqr_get_global_dataset <- bq_get_global_dataset
