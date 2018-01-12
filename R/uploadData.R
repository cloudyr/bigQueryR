#' Upload data to BigQuery
#' 
#' @param projectId The BigQuery project ID.
#' @param datasetId A datasetId within projectId.
#' @param tableId ID of table where data will end up.
#' @param upload_data The data to upload, a data.frame object or a Google Cloud Storage URI
#' @param create Whether to create a new table if necessary, or error if it already exists.
#' @param overwrite If TRUE will delete any existing table and upload new data.
#' @param schema If \code{upload_data} is a Google Cloud Storage URI, supply the data schema.  For \code{CSV} a helper function is available by using \link{schema_fields} on a data sample
#' @param sourceFormat If \code{upload_data} is a Google Cloud Storage URI, supply the data format.  Default is \code{CSV}
#' @param wait If uploading a data.frame, whether to wait for it to upload before returning
#' @param autodetect Experimental feature that auto-detects schema for CSV and JSON files
#' @param nullMarker Specifies a string that represents a null value in a CSV file. 
#'   For example, if you specify \code{\\N}, BigQuery interprets \code{\\N} as a null value when loading a CSV file. The default value is the empty string. 
#' @param maxBadRecords The maximum number of bad records that BigQuery can ignore when running the job
#' 
#' @return TRUE if successful, FALSE if not. 
#' 
#' @seealso url{https://cloud.google.com/bigquery/loading-data-post-request}
#' 
#' @details 
#' 
#' A temporary csv file is created when uploading from a local data.frame
#' 
#' For larger file sizes up to 5TB, upload to Google Cloud Storage first via \link[googleCloudStorageR]{gcs_upload} then supply the object URI of the form \code{gs://project-name/object-name} to the \code{upload_data} argument.  
#'   
#' You also need to supply a data schema.  Remember that the file should not have a header row.
#'   
#' @examples 
#' 
#' \dontrun{
#' 
#'  library(googleCloudStorageR)
#'  library(bigQueryR)
#'  
#'  gcs_global_bucket("your-project")
#'  
#'  ## custom upload function to ignore quotes and column headers
#'  f <- function(input, output) {
#'    write.table(input, sep = ",", col.names = FALSE, row.names = FALSE, 
#'                quote = FALSE, file = output, qmethod = "double")}
#'    
#'  ## upload files to Google Cloud Storage
#'  gcs_upload(mtcars, name = "mtcars_test1.csv", object_function = f)
#'  gcs_upload(mtcars, name = "mtcars_test2.csv", object_function = f)
#'  
#'  ## create the schema of the files you just uploaded
#'  user_schema <- schema_fields(mtcars)
#'  
#'  ## load files from Google Cloud Storage into BigQuery
#'  bqr_upload_data(projectId = "your-project", 
#'                 datasetId = "test", 
#'                 tableId = "from_gcs_mtcars", 
#'                 upload_data = c("gs://your-project/mtcars_test1.csv", 
#'                                 "gs://your-project/mtcars_test2.csv"),
#'                 schema = user_schema)
#' 
#' 
#' 
#' }
#' 
#' @family bigQuery upload functions
#' @export
#' @import assertthat
bqr_upload_data <- function(projectId = bqr_get_global_project(), 
                            datasetId = bqr_get_global_dataset(), 
                            tableId, 
                            upload_data, 
                            create = c("CREATE_IF_NEEDED", "CREATE_NEVER"),
                            overwrite = FALSE,
                            schema = NULL,
                            sourceFormat = c("CSV", "DATASTORE_BACKUP", 
                                             "NEWLINE_DELIMITED_JSON","AVRO"),
                            wait = TRUE,
                            autodetect = FALSE,
                            nullMarker = NULL,
                            maxBadRecords = NULL){
  

  assert_that(is.string(projectId),
              is.string(datasetId),
              is.string(tableId),
              is.flag(overwrite),
              is.flag(wait))
  sourceFormat <- match.arg(sourceFormat)
  create <- match.arg(create)
  
  if(inherits(upload_data, "data.frame")){
    myMessage("Uploading local data.frame", level = 3)
  } else if(inherits(upload_data, "character")){
    myMessage("Uploading from Google Cloud Storage URI", level = 3)
    stopifnot(all(grepl("^gs://", upload_data)))
    
    if(is.null(schema) && !autodetect){
      stop("Must supply a data schema or use autodetect if loading from Google Cloud Storage - see ?schema_fields")
    }
  }
  
  if(overwrite){
    deleted <- bqr_delete_table(projectId = projectId,
                                datasetId = datasetId,
                                tableId = tableId)
    
    if(!deleted) stop("Couldn't delete table")
  }
  
  bqr_do_upload(upload_data = upload_data, 
                projectId = projectId,
                datasetId = datasetId,
                tableId = tableId,
                create = create,
                user_schema = schema,
                sourceFormat = sourceFormat,
                wait = wait,
                autodetect = autodetect,
                nullMarker = nullMarker,
                maxBadRecords = maxBadRecords)
  
}

# S3 generic dispatch
bqr_do_upload <- function(upload_data, 
                          projectId, 
                          datasetId, 
                          tableId,
                          create,
                          user_schema,
                          sourceFormat,
                          wait,
                          autodetect,
                          nullMarker,
                          maxBadRecords){
  check_bq_auth()
  UseMethod("bqr_do_upload", upload_data)
}

# upload for local data.fram
bqr_do_upload.data.frame <- function(upload_data, 
                                     projectId, 
                                     datasetId, 
                                     tableId,
                                     create,
                                     user_schema,
                                     sourceFormat, # not used
                                     wait,
                                     autodetect,
                                     nullMarker,
                                     maxBadRecords){ 
  
  if(!is.null(user_schema)){
    schema <- user_schema
  } else {
    schema <- schema_fields(upload_data)
  }
  
  config <- list(
    configuration = list(
      load = list(
        nullMarker = nullMarker,
        maxBadRecords = maxBadRecords,
        sourceFormat = "CSV",
        createDisposition = jsonlite::unbox(create),
        schema = list(
          fields = schema
        ),
        destinationTable = list(
          projectId = projectId,
          datasetId = datasetId,
          tableId = tableId
        ),
        autodetect = autodetect
      )
    )
  )
  
  csv <- standard_csv(upload_data)
  
  boundary <- "--bqr_upload"
  line_break <- "\r\n"
  
  mp_body_schema <- paste(boundary,
                          "Content-Type: application/json; charset=UTF-8",
                          line_break,
                          jsonlite::toJSON(config, pretty=TRUE, auto_unbox = TRUE),
                          line_break,
                          sep = "\r\n")
  
  ## its very fussy about whitespace
  ## must match exactly https://cloud.google.com/bigquery/loading-data-post-request 
  mp_body_data <- paste0(boundary,
                         line_break,
                         "Content-Type: application/octet-stream",
                         line_break,
                         line_break,
                         csv)
  mp_body <- paste(mp_body_schema, mp_body_data, 
                   paste0(boundary, "--"), sep = "\r\n")
  
  l <- 
    googleAuthR::gar_api_generator("https://www.googleapis.com/upload/bigquery/v2",
                                   "POST",
                                   path_args = list(projects = projectId,
                                                    jobs = ""),
                                   pars_args = list(uploadType="multipart"),
                                   customConfig = list(
                                     httr::add_headers("Content-Type" = "multipart/related; boundary=bqr_upload"),
                                     httr::add_headers("Content-Length" = nchar(mp_body, type = "bytes"))
                                   )
    )
  
  req <- l(path_arguments = list(projects = projectId, 
                          datasets = datasetId,
                          tableId = tableId),
    the_body = mp_body)
  
  if(!is.null(req$content$status$errorResult)){
    stop("Error in upload job: ", req$status$errors$message)
  } else {
    myMessage("Upload job made...", level = 3)
  }
  
  if(req$status_code == 200){
    
    if(req$content$kind == "bigquery#job"){
      if(wait){
        out <- bqr_wait_for_job(as.job(req$content))
      } else {
        myMessage("Returning: BigQuery load of local data.frame Job object: ", 
                  req$content$jobReference$jobId, level = 3)
        
        out <- bqr_get_job(req$content$jobReference$jobId, 
                           projectId = req$content$jobReference$projectId)
      }

    } else {
      stop("Upload table didn't return bqr_job object when it should have.")
    }
    
  } else {
    myMessage("Error in upload, returning FALSE", level = 3)
    out <- FALSE
  }
  
  out
}

# upload for gs:// character vector
bqr_do_upload.character <- function(upload_data, 
                                    projectId, 
                                    datasetId, 
                                    tableId,
                                    create,
                                    user_schema,
                                    sourceFormat,
                                    wait, # not used
                                    autodetect,
                                    nullMarker,
                                    maxBadRecords){
  
  if(length(upload_data) > 1){
    source_uri <- upload_data
  } else {
    source_uri <- list(upload_data)
  }
  
  config <- list(
    configuration = list(
      load = list(
        nullMarker = nullMarker,
        maxBadRecords = maxBadRecords,
        sourceFormat = sourceFormat,
        createDisposition = jsonlite::unbox(create),
        sourceUris = source_uri,
        schema = list(
          fields = user_schema
        ),
        destinationTable = list(
          projectId = projectId,
          datasetId = datasetId,
          tableId = tableId
        ),
        autodetect = autodetect
      )
    )
  )
  
  
  l <- 
    googleAuthR::gar_api_generator("https://www.googleapis.com/bigquery/v2",
                                   "POST",
                                   path_args = list(projects = projectId,
                                                    jobs = "")
                                   )
  
  req <- l(path_arguments = list(projects = projectId, 
                          datasets = datasetId,
                          tableId = tableId),
    the_body = config)
  
  myMessage("Returning: BigQuery load from Google Cloud Storage Job object: ", 
            req$content$jobReference$jobId, level = 3)
  
  bqr_get_job(req$content$jobReference$jobId, projectId = req$content$jobReference$projectId)

}

#' Create data schema for upload to BigQuery
#' 
#' Use this on a sample of the data you want to load from Google Cloud Storage
#' 
#' @param data An example of the data to create a schema from
#' 
#' @return A schema object suitable to pass within the \code{schema} argument of \link{bqr_upload_data}
#' 
#' @details 
#' 
#' This is taken from \link[bigrquery]{insert_upload_job}
#' @author Hadley Wickham \email{hadley@@rstudio.com}
#'
#' @export
schema_fields <- function(data) { 
  types <- vapply(data, data_type, character(1))
  unname(Map(function(type, name) list(name = name, type = type), types, names(data)))
}

## From bigrquery
data_type <- function(x) {
  switch(class(x)[1],
         character = "STRING",
         logical = "BOOLEAN",
         numeric = "FLOAT",
         integer = "INTEGER",
         factor = "STRING",
         Date = "DATE",
         POSIXct = "TIMESTAMP",
         hms = "INTEGER",
         difftime = "INTEGER",
         stop("Unknown class ", paste0(class(x), collapse = "/"))
  )
}

## From bigrquery
## CSV load options https://cloud.google.com/bigquery/loading-data-into-bigquery#csvformat
standard_csv <- function(values) {
  # Convert factors to strings
  is_factor <- vapply(values, is.factor, logical(1))
  values[is_factor] <- lapply(values[is_factor], as.character)
  
  # Encode special characters in strings
  is_char <- vapply(values, is.character, logical(1))
  values[is_char] <- lapply(values[is_char], encodeString, na.encode = FALSE)
  
  # Encode dates and times
  is_time <- vapply(values, function(x) inherits(x, "POSIXct"), logical(1))
  values[is_time] <- lapply(values[is_time], as.numeric)
  
  # is_date <- vapply(values, function(x) inherits(x, "Date"), logical(1))
  # values[is_date] <- lapply(values[is_date], function(x) as.numeric(as.POSIXct(x)))
  
  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp))
  
  conn <- file(tmp, open = "wb")
  utils::write.table(values, conn, sep = ",", na = "", qmethod = "double",
                     row.names = FALSE, col.names = FALSE, eol = "\12")
  close(conn)
  
  # Don't read trailing nl
  readChar(tmp, file.info(tmp)$size - 1, useBytes = TRUE)
}