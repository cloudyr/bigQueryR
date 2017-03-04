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
  
  if(!is.null(x$configuration)){
    cat("\n# Job Configuration:\n")
    print(x$configuration)
  }
  
}