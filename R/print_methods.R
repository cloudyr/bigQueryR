#' @export
print.bqr_job <- function(x, ...){
  cat("==Google BigQuery Job==\n")
  cat0("JobID:          ", x$jobReference$jobId)
  cat0("ProjectID:      ", x$jobReference$projectId)
  cat0("Status:         ", x$status$state)
  cat0("User:           ", x$user_email)
  cat0("Created:        ", as.character(js_to_posix(x$statistics$creationTime)))
  cat0("Start:          ", as.character(js_to_posix(x$statistics$startTime)))
  cat0("End:            ", as.character(js_to_posix(x$statistics$endTime)))
  cat("## View job configuration via job$configuration\n")
  
  cat0("## Job had error: \n", x$status$errorResult$message)
  if(!is.null(x$status$errors)){
    print(x$status$errors$message)
  }

  
}