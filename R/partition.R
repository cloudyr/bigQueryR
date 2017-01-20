#' Convert date-sharded tables to a single partitioned table
#' 
#' Moves the old style date-shareded tables such as \code{[TABLE_NAME]_YYYYMMDD} to the new date partitioned format.
#' 
#' @param sharded The prefix of date-sharded tables to merge into one partitioned table
#' @param partition Name of partitioned table. Will create if not present already
#' @param project The project ID
#' @param dataset The dataset ID
#' 
#' @details 
#' 
#' WARNING: This can be an expensive operation for large datasets as it does a full column scan.
#' 
#' From \href{partitioned tables background}{https://cloud.google.com/bigquery/docs/partitioned-tables}: 
#' 
#' Replicates the functionality of the bq tool 
#' \code{bq query --allow_large_results --replace --noflatten_results --destination_table 'mydataset.table1$20160301' 'SELECT field1 + 10, field2 FROM mydataset.table1$20160301'}
#' 
#' Before partitioned tables became available, BigQuery users would often divide 
#'   large datasets into separate tables organized by time period; usually daily tables, 
#'   where each table represented data loaded on that particular date.
#'   
#' Dividing a dataset into daily tables helped to reduce the amount of data scanned 
#'   when querying a specific date range. For example, if you have a a year's worth of data 
#'   in a single table, a query that involves the last seven days of data still requires 
#'   a full scan of the entire table to determine which data to return. 
#'   However, if your table is divided into daily tables, you can restrict the query to 
#'   the seven most recent daily tables.
#'   
#' Daily tables, however, have several disadvantages. You must manually, or programmatically, 
#'   create the daily tables. SQL queries are often more complex because your data can be 
#'   spread across hundreds of tables. Performance degrades as the number of referenced 
#'   tables increases. There is also a limit of 1,000 tables that can be referenced in a 
#'   single query. Partitioned tables have none of these disadvantages.
#' 
#' @return \code{TRUE} if all parition jobs start running successfully
#' 
#' @seealso \href{Partitioned Tables Help}{https://cloud.google.com/bigquery/docs/creating-partitioned-tables}
#' @export
bqr_partition <- function(sharded,
                          partition,
                          projectId,
                          datasetId){
  
  ## check for shared tables
  tables <- bqr_list_tables(projectId = projectId, datasetId = datasetId)
  
  shard_tables <- tables[grepl(paste0("^",sharded), tables$tableId),]
  if(nrow(shard_tables) == 0){
    stop("No sharded tables not found - is your tableID correct? Got ", sharded)
  }
  
  ## check for partition table, creating if not there
  part_table <- tables[grepl(paste0("^",partition,"$"), tables$tableId),"tableId"]
  if(length(part_table) == 0){
    myMessage("Creating Partition Table: ", partition, level = 3)
    
    shard_schema <- bqr_query(projectId = projectId, 
                              datasetId = datasetId, 
                              query = sprintf('SELECT * FROM %s LIMIT 1', shard_tables$tableId[[1]]))
    
    part_table <- bqr_create_table(projectId = projectId,
                                   datasetId = datasetId,
                                   tableId = partition,
                                   template_data = shard_schema,
                                   timePartitioning = TRUE)
  }
  ## extract shard dates
  ex <- function(x) {
    gsub(".+?([0-9]{8}$)","\\1",x)
  }
  
  shard_dates <- vapply(shard_tables$tableId, ex, character(1), USE.NAMES = TRUE)
  
  ## query sharded tables, putting results in partition table
  part_query <- function(sdn){
    
    myMessage("Partitioning ", sdn, level = 3)
    ## build queries
    bqr_query_asynch(projectId = projectId,
                     datasetId = datasetId,
                     query = paste0('SELECT * FROM ',sdn),
                     destinationTableId = paste0(partition,"$",shard_dates[[sdn]]))
  }
  
  result <- lapply(names(shard_dates), part_query)
  
  if(all(vapply(result, function(x) x$status$state, character(1))) == "RUNNING"){
    myMessage("All partition jobs running, check project job queue for outcome.")
  }
  
  TRUE
  
}
