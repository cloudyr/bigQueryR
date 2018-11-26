#' List BigQuery datasets
#' 
#' Each projectId can have multiple datasets.
#' 
#' @param projectId The BigQuery project ID
#' 
#' @examples 
#' 
#' \dontrun{
#'   library(bigQueryR)
#'   
#'   ## this will open your browser
#'   ## Authenticate with an email that has access to the BigQuery project you need
#'   bqr_auth()
#'   
#'   ## verify under a new user
#'   bqr_auth(new_user=TRUE)
#'   
#'   ## get projects
#'   projects <- bqr_list_projects()
#'   
#'   my_project <- projects[1]
#'   
#'   ## for first project, get datasets
#'   datasets <- bqr_list_datasets[my_project]
#'   
#' }
#' 
#' @family bigQuery meta functions
#' @importFrom googleAuthR gar_api_generator gar_api_page
#' @export
bqr_list_datasets <- function(projectId = bqr_get_global_project()){
  
  check_bq_auth()
  l <- gar_api_generator("https://www.googleapis.com/bigquery/v2",
                         "GET",
                         path_args = list(projects = projectId,
                                          datasets = ""),
                         pars_args = list(pageToken=""),
                         data_parse_function = parse_list_datasets)
  pages <- gar_api_page(l, 
                        page_f = get_attr_nextpagetoken,
                        page_method = "param",
                        page_arg = "pageToken")
  
  Reduce(rbind, pages)
  
}

#' @import assertthat
#' @noRd
parse_list_datasets <- function(x){
  
  assert_that(x$kind == "bigquery#datasetList")
  
  if(!is.null(x$datasets)) {
    d <- x$datasets
    o <- data.frame(datasetId = d$datasetReference$datasetId,
                    id = d$id,
                    projectId = d$datasetReference$projectId,
                    location = d$location,
                    stringsAsFactors = FALSE)
  } else {
    o <- data.frame()
  }
  attr(o, "nextPageToken") <- x$nextPageToken
  o
}


#' Copy datasets
#' 
#' Uses \link[bqr_copy_table] to copy all the tables in a dataset.  
#' 
#' @param source_datasetid source datasetId
#' @param destination_datasetid destination datasetId
#' @param source_projectid source table's projectId
#' @param destination_projectid destination table's projectId
#' @param createDisposition Create table's behaviour
#' @param writeDisposition Write to an existing table's behaviour
#' 
#' @details 
#' 
#' You can't copy across dataset regions (e.g. EU to US), or copy BigQuery Views.
#' 
#' @export
#' @import assertthat
#' 
#' @return A named list of jobs of the source datasets, with details of job started. 
bqr_copy_dataset <- function(source_datasetid,
                             destination_datasetid,
                             source_projectid = bqr_get_global_project(),
                             destination_projectid = bqr_get_global_project(),
                             createDisposition = c("CREATE_IF_NEEDED","CREATE_NEVER"),
                             writeDisposition = c("WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY")){
  
  createDisposition <- match.arg(createDisposition)
  writeDisposition <- match.arg(writeDisposition)
  
  assert_that(
    is.string(source_projectid),
    is.string(source_datasetid),
    is.string(destination_projectid),
    is.string(destination_datasetid)
  )
  
  source_tables <- bqr_list_tables(source_projectid, 
                                   datasetId = source_datasetid, 
                                   maxResults = -1) 
  
  mapply(bqr_copy_table, 
         source_tableid = source_tables$tableId,
         destination_tableid = source_tables$tableId,
         MoreArgs = list(
           source_projectid = source_projectid,
           source_datasetid = source_datasetid,
           destination_projectid = destination_projectid,
           destination_datasetid = destination_datasetid,
           createDisposition = createDisposition,
           writeDisposition = writeDisposition
         ),
         SIMPLIFY = FALSE)
}