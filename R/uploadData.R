## see https://cloud.google.com/bigquery/loading-data-post-request

#' Upload data to BigQuery
#' 
#' @param projectId The BigQuery project ID.
#' @param datasetId A datasetId within projectId.
#' @param tableId Name of table you want.
#' @param upload_data The data to upload, a data.fame.
#' @param create If TRUE will create the table if it isn't present.
#' @param uploadType 'multipart' for small data, 'resumable' for big. (not implemented yet)
#' 
#' @return TRUE if successful, FALSE if not. 
#' 
#' @details 
#' 
#' A temporary csv file is creted if you choose a dataframe.
#' 
#' @export
bqr_upload_data <- function(projectId, 
                            datasetId, 
                            tableId, 
                            upload_data, 
                            create = TRUE,
                            uploadType = c("multipart","resumable")){
  
  stopifnot(inherits(upload_data, "data.frame"))
  
  if(create){
    creation <- bqr_create_table(projectId = projectId,
                                 datasetId = datasetId,
                                 tableId = tableId,
                                 template_data = upload_data)
    
    if(!creation) stop("Can't upload: Table already exisits.")
  }
  
  config <- list(
    configuration = list(
      load = list(
        sourceFormat = "CSV",
        schema = list(
          fields = schema_fields(upload_data)
        ),
        destinationTable = list(
          projectId = projectId,
          datasetId = datasetId,
          tableId = tableId
        )
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
  mp_body <- paste(mp_body_schema, mp_body_data, paste0(boundary, "--"), sep = "\r\n")
  
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
  
  if(req$status_code == 200){
    message("Upload request successful")
    out <- TRUE
  } else {
#     warning("Error in upload: ", req$status_code, " Returning request for debugging ")
#     out <- req
    out <- FALSE
  }
  
  out
  
}


## From bigrquery
## https://github.com/rstats-db/bigrquery/blob/master/R/upload.r 
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
         Date = "TIMESTAMP",
         POSIXct = "TIMESTAMP",
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
  
  is_date <- vapply(values, function(x) inherits(x, "Date"), logical(1))
  values[is_date] <- lapply(values[is_date], function(x) as.numeric(as.POSIXct(x)))
  
  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp))
  
  conn <- file(tmp, open = "wb")
  write.table(values, conn, sep = ",", na = "", qmethod = "double",
              row.names = FALSE, col.names = FALSE, eol = "\12")
  close(conn)
  
  # Don't read trailing nl
  readChar(tmp, file.info(tmp)$size - 1, useBytes = TRUE)
}