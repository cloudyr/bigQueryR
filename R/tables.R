#' Copy BigQuery table
#' 
#' Copy a source table to another destination
#' 
#' @param source_projectid source table's projectId
#' @param source_datasetid source table's datasetId
#' @param source_tableid source table's tableId
#' @param destination_projectid destination table's projectId
#' @param destination_datasetid destination table's datasetId
#' @param destination_tableid destination table's tableId
#' @param createDisposition Create table's behaviour
#' @param writeDisposition Write to an existing table's behaviour
#' 
#' @return A job object
#' 
#' @export
#' @import assertthat
#' @family Table meta functions
bqr_copy_table <- function(source_tableid,
                           destination_tableid,
                           source_projectid = bqr_get_global_project(),
                           source_datasetid = bqr_get_global_dataset(),
                           destination_projectid = bqr_get_global_project(),
                           destination_datasetid = bqr_get_global_dataset(),
                           createDisposition = c("CREATE_IF_NEEDED","CREATE_NEVER"),
                           writeDisposition = c("WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY")){
  
  createDisposition <- match.arg(createDisposition)
  writeDisposition <- match.arg(writeDisposition)
  
  assert_that(
    is.string(source_projectid),
    is.string(source_datasetid),
    is.string(source_tableid),
    is.string(destination_projectid),
    is.string(destination_datasetid),
    is.string(destination_tableid)
  )
  
  config <- list(
    configuration = list(
      copy = list(
        createDisposition = createDisposition,
        sourceTable = list(
          projectId = source_projectid,
          datasetId = source_datasetid,
          tableId = source_tableid
        ),
        destinationTable = list(
          projectId = destination_projectid,
          datasetId = destination_datasetid,
          tableId = destination_tableid
        ),
        writeDisposition = writeDisposition
      )
    )
  )
  
  myMessage(sprintf("Copying table %s.%s.%s to %s.%s.%s", 
                    source_projectid, source_datasetid, source_tableid, 
                    destination_projectid,destination_datasetid, destination_tableid),
            level = 3)
  
  call_job(source_projectid, config = config)
}



#' List BigQuery tables in a dataset
#' 
#' @param projectId The BigQuery project ID
#' @param datasetId A datasetId within projectId
#' @param maxResults Number of results to return, default \code{-1} returns all results
#' 
#' @return dataframe of tables in dataset
#' 
#' @examples 
#' 
#' \dontrun{
#'  bqr_list_tables("publicdata", "samples")
#' }
#' 
#' @family Table meta functions
#' @import assertthat
#' @importFrom googleAuthR gar_api_generator gar_api_page
#' @export
bqr_list_tables <- function(projectId = bqr_get_global_project(), 
                            datasetId = bqr_get_global_dataset(),
                            maxResults = -1){
  
  assert_that(is.string(projectId),
              is.string(datasetId),
              is.scalar(maxResults))
  
  # support -1 for all results
  if(maxResults < 0){
    maxResults=NULL
  }
  
  pars <- list(maxResults = maxResults,
               pageToken = "")
  pars <- rmNullObs(pars)
  
  
  check_bq_auth()
  l <- gar_api_generator("https://bigquery.googleapis.com/bigquery/v2",
                         "GET",
                         path_args = list(projects = projectId,
                                          datasets = datasetId,
                                          tables = ""),
                         pars_args = pars,
                         data_parse_function = parse_bqr_list_tables)
  
  pages <- gar_api_page(l, 
                        page_f = get_attr_nextpagetoken,
                        page_method = "param",
                        page_arg = "pageToken")
  
  Reduce(rbind, pages)

}

parse_bqr_list_tables <- function(x) {
  d <- x$tables
  out <- data.frame(id = d$id,
                    projectId = d$tableReference$projectId,
                    datasetId = d$tableReference$datasetId,
                    tableId = d$tableReference$tableId, stringsAsFactors = FALSE)
  
  if(!is.null(x$nextPageToken)){
    attr(out, "nextPageToken") <- x$nextPageToken
  }
  
  out

  
}

#' Get BigQuery Table meta data
#' 
#' @param projectId The BigQuery project ID
#' @param datasetId A datasetId within projectId
#' @param tableId The tableId within the datasetId
#' 
#' @return list of table metadata
#' 
#' @examples 
#' 
#' \dontrun{
#'   bqr_table_meta("publicdata", "samples", "github_nested")
#' }
#' 
#' 
#' @family Table meta functions
#' @export
bqr_table_meta <- function(projectId = bqr_get_global_project(), 
                           datasetId = bqr_get_global_dataset(), 
                           tableId){
  
  check_bq_auth()
  f <- function(x){
    x <- rmNullObs(x)
  }
  
  
  l <- googleAuthR::gar_api_generator("https://bigquery.googleapis.com/bigquery/v2",
                                      "GET",
                                      path_args = list(projects = projectId,
                                                       datasets = datasetId,
                                                       tables = tableId),
                                      data_parse_function = f)
  
  res <- l(path_arguments = list(projects = projectId, 
                          datasets = datasetId, 
                          tables = tableId))
  
  as.table(res)
  
}

#' Get BigQuery Table's data list
#' 
#' @param projectId The BigQuery project ID
#' @param datasetId A datasetId within projectId
#' @param tableId The tableId within the datasetId
#' @param maxResults Number of results to return
#' 
#' @return data.frame of table data
#' 
#' This won't work with nested datasets, for that use \link{bqr_query} as that flattens results.
#' 
#' @family Table meta functions
#' @export
bqr_table_data <- function(projectId = bqr_get_global_project(), 
                           datasetId = bqr_get_global_dataset(), 
                           tableId,
                           maxResults = 1000){
  check_bq_auth()
  l <- googleAuthR::gar_api_generator("https://bigquery.googleapis.com/bigquery/v2",
                                      "GET",
                                      path_args = list(projects = projectId,
                                                       datasets = datasetId,
                                                       tables = tableId,
                                                       data = ""),
                                      pars_args = list(maxResults = maxResults),
                                      data_parse_function = function(x) x)
  
  l(path_arguments = list(projects = projectId, 
                          datasets = datasetId, 
                          tables = tableId),
    pars_arguments = list(maxResults = maxResults))
  
}


#' Create a Table
#' 
#' @param projectId The BigQuery project ID.
#' @param datasetId A datasetId within projectId.
#' @param tableId Name of table you want.
#' @param template_data A dataframe with the correct types of data. If \code{NULL} an empty table is made.
#' @param timePartitioning Whether to create a partioned table
#' @param expirationMs If a partioned table, whether to have an expiration time on the data. The default \code{0} is no expiration.
#' 
#' @return TRUE if created, FALSE if not.  
#' 
#' @details 
#' 
#' Creates a BigQuery table.
#' 
#' If setting \code{timePartioning} to \code{TRUE} then the table will be a 
#'   \href{https://cloud.google.com/bigquery/docs/creating-partitioned-tables}{partioned table}
#'   
#' If you want more advanced features for the table, create it then call \link{bqr_patch_table} with advanced configuration configured from \link{Table}
#' 
#' @family Table meta functions
#' @export
bqr_create_table <- function(projectId = bqr_get_global_project(), 
                             datasetId = bqr_get_global_dataset(), 
                             tableId, 
                             template_data = NULL,
                             timePartitioning = FALSE,
                             expirationMs = 0L){
  check_bq_auth()
  l <- googleAuthR::gar_api_generator("https://bigquery.googleapis.com/bigquery/v2",
                                      "POST",
                                      path_args = list(projects = projectId,
                                                       datasets = datasetId,
                                                       tables = "")
                                      )
  expirationMs <- as.integer(expirationMs)
  timeP <- NULL
  if(timePartitioning){
    if(expirationMs == 0) expirationMs <- NULL
    timeP <- list(type = "DAY", expirationMs = expirationMs)
  }
  
  if(!is.null(template_data)){
    schema <- list(
      fields = schema_fields(template_data)
    )
  } else {
    schema <- NULL
  }
  
  config <- list(
        schema = schema,
        tableReference = list(
          projectId = projectId,
          datasetId = datasetId,
          tableId = tableId
        ),
        timePartitioning = timeP
  )
  
  config <- rmNullObs(config)
  
  req <- try(l(path_arguments = list(projects = projectId, 
                                     datasets = datasetId),
               the_body = config), silent = TRUE)
  
  if(is.error(req)){
    if(grepl("Already Exists", error.message(req))){
      message("Table exists: ", tableId, "Returning FALSE")
      out <- FALSE
    } else {
      stop(error.message(req))
    }
  } else {
    message("Table created: ", tableId)
    out <- TRUE
  }
  
  out
  
}

#' Update a Table
#' 
#' @param Table A Table object as created by \link{Table}
#' 
#' @description 
#'  This uses PATCH semantics to alter an existing table.  
#'  You need to create the Table object first to pass in using \link{Table} 
#'which will be transformed to JSON
#' 
#' @export
#' @import assertthat
#' @importFrom googleAuthR gar_api_generator
#' @seealso \href{https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource}{Definition of tables}
#' @family Table meta functions
bqr_patch_table <- function(Table){
  assert_that(
    is.table(Table)
  )
  
  projectId <- Table$tableReference$projectId
  datasetId <- Table$tableReference$datasetId
  tableId <- Table$tableReference$tableId
  
  myMessage("Patching ", tableId, level = 3)
  
  the_url <- sprintf("https://bigquery.googleapis.com/bigquery/v2/projects/%s/datasets/%s/tables/%s",
                     projectId, datasetId, tableId)
  
  call_api <- gar_api_generator(the_url, "PATCH", data_parse_function = function(x) x)
  
  res <- call_api(the_body = Table)
  
  as.table(res)
  
}

#' Delete a Table
#' 
#' @param projectId The BigQuery project ID.
#' @param datasetId A datasetId within projectId.
#' @param tableId Name of table you want to delete.
#' 
#' @return TRUE if deleted, FALSE if not.  
#' 
#' @details 
#' 
#' Deletes a BigQuery table
#' 
#' @family Table meta functions
#' @export
bqr_delete_table <- function(projectId = bqr_get_global_project(), 
                             datasetId = bqr_get_global_dataset(), 
                             tableId){
  check_bq_auth()
  l <- googleAuthR::gar_api_generator("https://bigquery.googleapis.com/bigquery/v2",
                                      "DELETE",
                                      path_args = list(projects = projectId,
                                                       datasets = datasetId,
                                                       tables = tableId)
  )
  
  req <- try(suppressWarnings(l(path_arguments = list(projects = projectId, 
                                           datasets = datasetId,
                                           tables = tableId))), silent = TRUE)
  if(is.error(req)){
    if(grepl("Not found", error.message(req))){
      myMessage(error.message(req), level = 3)
      out <- FALSE
    } else {
      stop(error.message(req))
    }
  } else {
    out <- TRUE
  }
  
  out
  
}


#' Table Object
#' 
#' Configure table objects as documented by 
#' the \href{https://cloud.google.com/bigquery/docs/reference/rest/v2/tables}{Google docs for Table objects}
#' 
#' @param tableId tableId 
#' @param projectId projectId
#' @param datasetId datasetId
#' @param clustering [Beta] Clustering specification for the table
#' @param description [Optional] A user-friendly description of this table
#' @param encryptionConfiguration Custom encryption configuration (e
#' @param expirationTime [Optional] The time when this table expires, in milliseconds since the epoch
#' @param friendlyName [Optional] A descriptive name for this table
#' @param labels The labels associated with this table - a named list of key = value
#' @param materializedView [Optional] Materialized view definition
#' @param rangePartitioning [TrustedTester] Range partitioning specification for this table
#' @param requirePartitionFilter [Beta] [Optional] If set to true, queries over this table require a partition filter that can be used for partition elimination to be specified
#' @param schema [Optional] Describes the schema of this table
#' @param timePartitioning Time-based partitioning specification for this table
#' @param view [Optional] The view definition
#' 
#' @return Table object
#' 
#' @details 
#' 
#' A table object to be used within \link{bqr_patch_table}
#' 
#' @family Table meta functions
#' @export
#' @import assertthat
Table <- function(tableId,
                  projectId = bqr_get_global_project(), 
                  datasetId = bqr_get_global_dataset(), 
                  clustering = NULL, 
                  description = NULL, 
                  encryptionConfiguration = NULL, 
                  expirationTime = NULL, 
                  friendlyName = NULL, 
                  labels = NULL, 
                  materializedView = NULL, 
                  rangePartitioning = NULL, 
                  requirePartitionFilter = NULL, 
                  schema = NULL, 
                  timePartitioning = NULL, 
                  view = NULL) {
  assert_that(
    is.string(projectId),
    is.string(datasetId),
    is.string(tableId)
    # is.string(friendlyName),
    # is.string(description),
    # is.list(labels),
    # is.list(timePartitioning)
    # is.flag(requirePartitionFilter)
  )
  
  tt <- list(
    tableReference = list(projectId = projectId,
                          datasetId = datasetId,
                          tableId = tableId),
    clustering = clustering, 
    description = description, 
    encryptionConfiguration = encryptionConfiguration, 
    expirationTime = expirationTime,
    friendlyName = friendlyName, 
    labels = labels, 
    materializedView = materializedView, 
    rangePartitioning = rangePartitioning, 
    requirePartitionFilter = NULL, 
    schema = schema,
    timePartitioning = timePartitioning, 
    view = view)
  
  tt <- rmNullObs(tt)
  
  structure(tt, class = "gar_Table")
  
}

is.table <- function(x){
  inherits(x, "gar_Table")
}

as.table <- function(x){
  structure(x, class = "gar_Table")
}