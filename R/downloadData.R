#' Extract data asynchronously
#' 
#' Use this instead of \link{bqr_query} for big datasets. 
#' Requires you to make a bucket at https://console.cloud.google.com/storage/browser
#' 
#' @param projectId The BigQuery project ID.
#' @param datasetId A datasetId within projectId.
#' @param tableId ID of table you wish to extract.
#' @param cloudStorageBucket URI of the bucket to extract into.
#' @param filename Include a wildcard (*) if extract expected to be > 1GB.
#' @param compression Compression of file.
#' @param destinationFormat Format of file.
#' @param fieldDelimiter fieldDelimiter of file.
#' @param printHeader Whether to include header row.
#'  
#' @seealso 
#'   https://cloud.google.com/bigquery/exporting-data-from-bigquery
#'   
#' @return A Job object to be queried via \link{bqr_get_job}
#'   
#' @export
bqr_extract_data <- function(projectId, 
                             datasetId, 
                             tableId,
                             cloudStorageBucket,
                             filename = paste0("big-query-extract-",
                                               gsub(" |:|-","", 
                                                    Sys.time()),"-*.csv"),
                             compression = c("NONE","GZIP"),
                             destinationFormat = c("CSV",
                                                   "NEWLINE_DELIMITED_JSON", 
                                                   "AVRO"),
                             fieldDelimiter = ",",
                             printHeader = TRUE){
  
  compression <- match.arg(compression)
  destinationFormat <- match.arg(destinationFormat)
  
  stopifnot(inherits(projectId, "character"),
            inherits(datasetId, "character"),
            inherits(tableId, "character"),
            inherits(cloudStorageBucket, "character"),
            inherits(filename, "character"),
            inherits(fieldDelimiter, "character"),
            inherits(printHeader, "logical"))
  
  if(!grepl("^gs://",cloudStorageBucket)) 
    cloudStorageBucket <- paste0("gs://", cloudStorageBucket)
  
  ## make job
  job <- 
    googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                   "POST",
                                   path_args = list(projects = projectId,
                                                    jobs = "")
                                   )

  gsUri <- paste0(cloudStorageBucket, "/", filename)
  
  config <- list(
    jobReference = list(
      projectId = projectId,
      jobId = idempotency() ## uuid to stop duplicate exports
    ),
    configuration = list(
      extract = list(
        sourceTable = list(
          datasetId = datasetId,
          projectId = projectId,
          tableId = tableId
        ),
        destinationUris = list(
          gsUri
          ),
        printHeader = printHeader,
        fieldDelimiter = fieldDelimiter,
        destinationFormat = destinationFormat,
        compression = compression
      )
    )
  )
  
  config <- rmNullObs(config)
  
  req <- job(path_arguments = list(projects = projectId),
               the_body = config)
  
  if(req$status_code == 200){
    myMessage("Extract request successful", level=2)
    out <- req$content
  } else {
    stop("Error in extraction job")
    # out <- FALSE
  }
  
  out

}

#' Grant access to an extract on Google Cloud Storage
#' 
#' To access the data created in \link{bqr_extract_data}.
#' Requires the Google account email of the user. 
#' 
#' Uses \href{https://cloud.google.com/storage/docs/authentication#cookieauth}{cookie based auth}.
#' 
#' 
#' @param extractJob An extract job from \link{bqr_extract_data}
#' @param email email of the user to have access
#' 
#' @return URL(s) to download the extract that is accessible by email
#' @export
bqr_grant_extract_access <- function(extractJob, email){
  
  if(extractJob$status$state != "DONE"){
    stop("Job not done")
  }
  
  stopifnot(inherits(email, "character"))
  
  ## if multiple files, create the suffixs 000000000000, 000000000001, etc.
  file_suffix <- make_suffix(extractJob$statistics$extract$destinationUriFileCounts)
  
  ## replace filename * with suffixes
  uris <- gsub("\\*", "%s", extractJob$configuration$extract$destinationUris)
  uris <- sprintf(uris, file_suffix)
  
  ## extract bucket names and object names
  bucketnames <- gsub("gs://(.+)/(.+)\\.csv$","\\1",uris)
  objectnames <- gsub("gs://(.+)/(.+)\\.csv$","\\2",uris)
  
  ## Update access control list of objects to accept the email
  
  # helper function with prefilled params
  updateAccess <- function(object){
    gcs_update_acl(bucket = bucketnames[1], # should all be in same bucket
                   object = paste0(object,".csv"),
                   entity = email,
                   entity_type = "user",
                   role = "READER")
  }
  
  result <- vapply(objectnames, updateAccess, logical(1))
  
  ## the download URLs
  downloadUri <- paste0("https://storage.cloud.google.com", 
                        "/",bucketnames,
                        "/", objectnames,
                        ".csv")
  
  if(all(result)){
    out <- downloadUri
  } else {
    warning("Problem setting access")
    out <- NULL
  }
  
  out
  
}

# Helper for filenames
make_suffix <- function(destinationUriFileCount){
  suff <- function(x) gsub(" ","0",sprintf("%12d", as.numeric(x)))
  along <- 0:(as.numeric(destinationUriFileCount)-1)

  vapply(along, suff, "000000000000")
}