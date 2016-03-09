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
                              printHeader = TRUE
                              ){
  
  compression <- match.arg(compression)
  destinationFormat <- match.arg(destinationFormat)
  
  
  ## make job
  job <- 
    googleAuthR::gar_api_generator("https://www.googleapis.com/upload/bigquery/v2",
                                   "POST",
                                   path_args = list(projects = projectId,
                                                    jobs = ""))

  gsUri <- paste0("gs://", cloudStorageBucket, "/", filename)
  
  config <- list(
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
    message("Extract request successful")
    out <- TRUE
  } else {
    #     warning("Error in upload: ", req$status_code, " Returning request for debugging ")
    #     out <- req
    out <- FALSE
  }
  
  out
  
  
  ## https://cloud.google.com/storage/docs/access-control
  ## create signed string for Gogole buxket resource
  ##StringToSign = HTTP_Verb + "\n" +
    #Content_MD5 + "\n" +
    #Content_Type + "\n" +
    #Expiration + "\n" +
    #Canonicalized_Extension_Headers +
    #Canonicalized_Resource
  
  
  #generating RSA signatures using SHA-256 as the hash function
  ##https://cran.r-project.org/web/packages/sodium/vignettes/intro.html
  ##https://cran.r-project.org/web/packages/digest/index.html
  # digest::digest(paste0("StringToSign = ",
  #               "HTTP_Verb","\n\n\n",
  #               "Expiration\n",
  #               "googlestorage.com/blah"), algo = "sha256")
  
}

#' Turn a BigQuery query into a table
#' 
#' Use for big results
#' 
#' 
bqr_query_asynch <- function(projectId, datasetId, query){
  
  
}