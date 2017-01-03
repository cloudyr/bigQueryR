#' Convert date-sharded tables to a single partitioned table
#' 
#' Moves the old style date-shareded tables such as \code{[TABLE_NAME]_YYYYMMDD} to the new date partitioned format.
#' 
#' @param sharded The date-sharded tables to merge into one partitioned table
#' @param partition Name of partitioned table. Will create if not present already
#' @param delete_sharded If \code{TRUE}, will delete the sharded tables when done
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
#' @return The partition table object
#' 
#' @seealso \href{Partitioned Tables Help}{https://cloud.google.com/bigquery/docs/creating-partitioned-tables}
#' @export
bqr_partition <- function(sharded,
                          partition,
                          projectId,
                          datasetId ,
                          delete_sharded = FALSE){
  
  ## check for shared tables
  tables <- bqr_list_tables(projectId = projectId, datasetId = datasetId)
  
  ## check for partition table, creating if not there
  
  ## extract shard dates
  
  ## build queries
  
  ## query sharded tables, putting results in partition table
  
  ## delete sharded tables
  
  ## return partition table details
  
}
