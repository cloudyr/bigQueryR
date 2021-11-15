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
#' @seealso \url{https://cloud.google.com/bigquery/exporting-data-from-bigquery}
#'
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
#' You should also see the extract in the Google Cloud Storage bucket
#' googleCloudStorageR::gcs_list_objects("your_cloud_storage_bucket_name")
#' 
#' ## to download via a URL and not logging in via Google Cloud Storage interface:
#' ## Use an email that is Google account enabled
#' ## Requires scopes:
#' ##  https://www.googleapis.com/auth/devstorage.full_control
#' ##  https://www.googleapis.com/auth/cloud-platform
#' 
#' download_url <- bqr_grant_extract_access(job_extract, "your@email.com")
#' 
#' ## download_url may be multiple if the data is > 1GB
#' 
#' }
#' 
#' @return A Job object to be queried via \link{bqr_get_job}
#'
#' @family BigQuery asynch query functions  
#' @export
bqr_extract_data <- function(projectId = bqr_get_global_project(), 
                             datasetId = bqr_get_global_dataset(), 
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
  
  check_gcs_auth()
  
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
      projectId = projectId
      ##jobId = idempotency() ## uuid to stop duplicate exports - breaks if set.seed (#37)
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
    myMessage("Extract request successful, use bqr_wait_for_job() to know when it is ready.", 
              level=3)
    out <- as.job(req$content)
  } else {
    stop("Error in extraction job")
    # out <- FALSE
  }
  
  out

}

#' Download extract data
#' 
#' After extracting data via \link{bqr_extract_data} download the 
#'   extract from the Google Storage bucket.
#'   
#' If more than 1GB, will save multiple .csv files with prefix "N_" to filename.
#' 
#' @param extractJob An extract job from \link{bqr_extract_data}
#' @param filename Where to save the csv file. If NULL then uses objectname.
#' 
#' @return TRUE if successfully downloaded
#' @import googleCloudStorageR
#' @family BigQuery asynch query functions 
#' @export
bqr_download_extract <- function(extractJob,
                                 filename = NULL){
  
  if(extractJob$status$state != "DONE"){
    stop("Job not done")
  }
  
  check_gcs_auth()
  
  ## if multiple files, create the suffixs 000000000000, 000000000001, etc.
  file_suffix <- make_suffix(extractJob$statistics$extract$destinationUriFileCounts)
  
  ## replace filename * with suffixes
  uris <- gsub("\\*", "%s", extractJob$configuration$extract$destinationUris)
  uris <- sprintf(uris, file_suffix)
  
  ## extract bucket names and object names
  bucketnames <- gsub("gs://(.+)/(.+)$","\\1",uris)
  objectnames <- gsub("gs://(.+)/(.+)$","\\2",uris)
  
  if(!is.null(filename)){
    stopifnot(inherits(filename, "character"))
  } else {
    filename <- objectnames
  }
  
  if(length(objectnames) > 1){
    message("Multiple files to download.")
    filename <- paste0(as.character(1:length(objectnames),"_",filename))
  }
  
  dl <- function(f_name){
    googleCloudStorageR::gcs_get_object(
      bucket = bucketnames[[1]],
      object_name = f_name,
      saveToDisk = f_name
    )
  }
  
  lapply(filename, dl)
  
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
#' @family BigQuery asynch query functions  
#' @export
bqr_grant_extract_access <- function(extractJob, email){
  
  check_gcs_auth()
  
  stopifnot(is.job(extractJob))
  
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
  bucketnames <- gsub("gs://(.+)/(.+)$","\\1",uris)
  objectnames <- gsub("gs://(.+)/(.+)$","\\2",uris)
  
  ## Update access control list of objects to accept the email
  
  # helper function with prefilled params
  updateAccess <- function(object){
    googleCloudStorageR::gcs_update_object_acl(
      object_name = object,
      bucket = bucketnames[[1]],
      entity = email,
      entity_type = "user",
      role = "READER"
    )
  }
  
  result <- vapply(objectnames, updateAccess, logical(1))
  
  ## the download URLs
  downloadUri <- googleCloudStorageR::gcs_download_url(object_name = objectnames, 
                                                       bucket = bucketnames)
  
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

