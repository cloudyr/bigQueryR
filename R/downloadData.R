#' Extract data asynchronously
#' 
#' Use this instead of \link{bqr_query} for big datasets. 
#' Requires you to make a bucket at https://console.cloud.google.com/storage/browser
#' 
#' @param projectId The BigQuery project ID.
#' @param datasetId A datasetId within projectId.
#' @param tableId Name of table you want.
#' @param cloudStorageBucket URI of the bucket to extract via.
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

#' Create a signed URL for Google Cloud Storage
#' 
#' To access the data created in \link{bqr_extract_data}.
#' Requires a service account .pem:
#' 
#'   1. Create a service account and download .p12 key
#'   2. Run in console: 
#'   openssl pkcs12 -in path/to/key.p12 -nodes -nocerts > path/to/key.pem
#' 
#' @param extractJob An extract job from \link{bqr_extract_data}
#' @param expiration How long the link is valid for in seconds
#' @param pem file path to a .pem service account key
#' @param email email of the Google service account
#' 
#' @return Signed URL(s)
#' @export
bqr_signed_url <- function(extractJob, expiration=86400, pem, email){
  
  if(extractJob$status$state != "DONE"){
    stop("Job not done")
  }
  
  stopifnot(file.exists(pem),
            inherits(email, "character"))
  
  file_suffix <- make_suffix(extractJob$statistics$extract$destinationUriFileCounts)
  
  expiration <- round(as.numeric(Sys.time() + expiration))
  
  uris <- gsub("\\*", "%s", extractJob$configuration$extract$destinationUris)
  uris <- sprintf(uris, file_suffix)
  
  # bucketname <- gsub("gs://(.+)/(.+)\\.csv$","\\1",uris)
  canonical_resource <- gsub("gs:/","", uris)
  
  ## a vector if many uris
  string_to_sign <- paste0("GET","\n",
                           "\n",
                           "\n",
                           expiration, "\n",
                           canonical_resource)
  
  myMessage(string_to_sign, level = 0)
  
  ## used PKI library to sign using the .pem file
  signed_strings <- vapply(string_to_sign, 
                           FUN = signRSA, 
                           FUN.VALUE = "vector", 
                           pem)
  
  baseUrls <- gsub("gs://","https://storage.googleapis.com/", uris)
  
  ## construct the final download URLs
  signed_urls <- paste0(baseUrls,
                        "?GoogleAccessId=", email,
                        "&Expires=", expiration,
                        "&Signature=",signed_strings)
  
  signed_urls
  
}

# Helper for filenames
make_suffix <- function(destinationUriFileCount){
  suff <- function(x) gsub(" ","0",sprintf("%12d", as.numeric(x)))
  along <- 0:(as.numeric(destinationUriFileCount)-1)

  vapply(along, suff, "vector")
}

# helper to sign via SHA-256 and RSA
# key needs to be in PEM or DER format
# GCS provides key files in
# pkcs12 format. To convert between formats, you can use the provided commands
# below.
#
# The default password for p12 file is `notasecret`
# 
# Given a GCS key in pkcs12 format, convert it to PEM using this command:
#   openssl pkcs12 -in path/to/key.p12 -nodes -nocerts > path/to/key.pem
# Given a GCS key in PEM format, convert it to DER format using this command:
#   openssl rsa -in privatekey.pem -inform PEM -out privatekey.der -outform DER
signRSA <- function(sign_me, pem){
  
  key <- openssl::read_key(pem, "notasecret")
  sig <- openssl::signature_create(charToRaw(sign_me), 
                                   hash = openssl::sha256, 
                                   key = key, 
                                   password = "notasecret")
  
  out <- utils::URLencode(paste0(sig, collapse =""), reserved = TRUE)
  
  out
}

