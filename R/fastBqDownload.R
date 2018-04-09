#' Download data from BigQuery to local folder
#' 
#' Requires you to make a bucket at https://console.cloud.google.com/storage/browser
#' 
#' @param query The query you want to run.
#' @param target_folder Target folder on your local computer.
#' @param result_file_name Name of your downloaded file. 
#' @param refetch Boolean, whether you would like to refetch previously downloaded data.
#' @param useLegacySql Boolean, whether to use Legacy SQL. Default is FALSE.
#' @param clean_intermediate_results Boolean, whether to keep intermediate files on BigQuery and Google Cloud Storage.
#' @param global_project_name BigQuery project name (where you would like to save your file during download).
#' @param global_dataset_name BigQuery dataset name (where you would like to save your file during download).
#' @param global_bucket_name Google Cloud Storage bucket name (where you would like to save your file during download).
#' 
#' @examples
#' 
#' \dontrun{
#' library(bigQueryR)
#' 
#' ## Auth with a project that has at least BigQuery and Google Cloud Storage scope
#' bqr_auth()
#' 
#' # Create a bucket at Google Cloud Storage at 
#' # https://console.cloud.google.com/storage/browser
#' 
#' bqr_download_query(query = "select * from `your_project.your_dataset.your_table`")
#' 
#' }
#' 
#' @return a data.table.
#' 
#' @export
#' @importFrom data.table fread
bqr_download_query <- function(query = NULL,
                               target_folder = "data",
                               result_file_name = NULL,
                               refetch = FALSE,
                               useLegacySql = FALSE,
                               clean_intermediate_results = TRUE,
                               global_project_name = bqr_get_global_project(),
                               global_dataset_name = bqr_get_global_dataset(),
                               global_bucket_name = googleCloudStorageR::gcs_get_global_bucket()
) {
    invisible(sapply(c("data.table", "purrr"), assertRequirement))

    if(is.null(result_file_name)){
        result_file_name <- "fast_bq_download_result"
    } else {
        result_file_name <- gsub("(\\.csv$)|(\\.csv\\.gz$)", "", result_file_name)
    }

    full_result_path <- file.path(target_folder, paste0(result_file_name, ".csv.gz"))
    if(file.exists(full_result_path) & !refetch){
        return(fread(paste("gunzip -c", full_result_path)))
    }

    setFastSqlDownloadOptions(global_project_name, global_dataset_name, global_bucket_name)

    gcp_result_name_raw <- paste0(result_file_name, "_", Sys.getenv("LOGNAME"), "_", Sys.time())
    gcp_result_name <- gsub("[^[:alnum:]]+", "_", gcp_result_name_raw)

    object_names <- saveQueryToStorage(query, gcp_result_name, useLegacySql)

    tryCatch(
        {
            output_dt <- readFromStorage(object_names, target_folder)
            unifyLocalChunks(output_dt, object_names, result_file_name, target_folder)
        },
        error = function(e) {
            myMessage("\n# Error while saving from Storage to local. Running cleanup of Storage and BigQuery. See original error message below:\n",
                      level = 3)
            myMessage(paste0(e, "\n"), level = 3)
        },
        finally = {if (clean_intermediate_results == TRUE) {
                    cleanIntermediateResults(object_names, gcp_result_name, target_folder)
            }
        }
    )

    output_dt
}


#' @noRd
#' @importFrom googleCloudStorageR gcs_global_bucket gcs_list_objects
setFastSqlDownloadOptions <- function(global_project_name, global_dataset_name, global_bucket_name) {
    # options(googleAuthR.scopes.selected = "https://www.googleapis.com/auth/cloud-platform")

    bqr_global_project(global_project_name)
    bqr_global_dataset(global_dataset_name)
    gcs_global_bucket(global_bucket_name)
}

#' @noRd
#' @importFrom googleCloudStorageR gcs_get_global_bucket
saveQueryToStorage <- function(query, result_name, useLegacySql){
    time <- Sys.time()
    myMessage("Querying data and saving to BigQuery table", level = 3)
    query_job <- bqr_query_asynch(
        query = query,
        useLegacySql = useLegacySql,
        destinationTableId = result_name,
        writeDisposition = "WRITE_TRUNCATE"
    )
    
    if(!is.null(query_job$status$errorResult)){
      stop("Query job error:", query_job$status$errorResult$message, call. = FALSE)
    }
    
    isDone <- suppressMessages(bqr_wait_for_job(query_job, wait = 2))$status$state == "DONE"

    if(isDone){
      time_elapsed <- difftime(Sys.time(), time)
      myMessage(paste("Querying job is finished, time elapsed:", format(time_elapsed,format = "%H:%M:%S")), level = 3)
      
      time <- Sys.time()
      myMessage("Writing data to storage", level = 3)
      extract_job <- suppressMessages(bqr_extract_data(tableId = result_name,
                                                       cloudStorageBucket = gcs_get_global_bucket(),
                                                       compression = "GZIP",
                                                       filename = paste0(result_name, "_*.csv.gz")
                                                       )
                                      )
    }
    
    isDone2 <- suppressMessages(bqr_wait_for_job(extract_job, wait = 2))$status$state == "DONE"

    if(isDone2){
        time_elapsed <- difftime(Sys.time(), time)
        myMessage(paste("Writing data to storage is finished, time elapsed:", format(time_elapsed,format = "%H:%M:%S")))
        object_names <- grep(result_name,
                             gcs_list_objects()$name,
                             value = TRUE)
    }
    
    object_names
}

#' @noRd
#' @importFrom googleCloudStorageR gcs_get_object
#' @importFrom purrr map
#' @importFrom data.table fread rbindlist
readFromStorage <- function(object_names, target_folder) {
    createFolder(target_folder)
    chunk_dt_list <- map(object_names, ~ {
        object <- .
        gcs_get_object(
            object_name = object,
            saveToDisk = file.path(target_folder, object),
            overwrite = TRUE
        )
        fread(paste0("gunzip -c ", file.path(target_folder,object)))
    })
    
    rbindlist(chunk_dt_list)
}

#' @noRd
#' @importFrom data.table fwrite
unifyLocalChunks <- function(output_dt, object_names, result_file_name, target_folder) {
    if(length(object_names) > 1) {
      full_result_file_name <- file.path(target_folder, paste0(result_file_name, ".csv"))
      
      fwrite(output_dt, full_result_file_name)
      gzipDataAtPath(full_result_file_name)
      
    } else{
        file.rename(
          file.path(target_folder, object_names[[1]]),
          file.path(target_folder, paste0(result_file_name, ".csv.gz"))
        )
    }
}

#' @noRd
#' @importFrom purrr walk
#' @importFrom googleCloudStorageR gcs_delete_object
cleanIntermediateResults <- function(object_names, table_id, target_folder){
  
    walk(object_names, ~ gcs_delete_object(object = .x))
  
    bqr_delete_table(tableId = table_id)
    
    if (length(object_names) > 1) {
        walk(file.path(target_folder, object_names), file.remove)
    }
    myMessage("The queried table on BigQuery and saved file(s) on GoogleCloudStorage have been cleaned up.
        If you want to keep them, use clean_intermediate_results = TRUE.", level = 3)
}

createFolder <- function(target_folder) {
    if (!dir.exists(target_folder)) {
        dir.create(target_folder, recursive = TRUE)
        myMessage(paste0(target_folder, ' folder does not exist. Creating folder.'), level = 3)
    }
}

gzipDataAtPath <- function(full_result_file_name) {
    system(paste0("rm -f ", full_result_file_name, ".gz"))
    system(paste0("gzip ", full_result_file_name))
}

assertRequirement <- function(package_name) {
    if (!requireNamespace(package_name, quietly = TRUE)) {
       stop(paste0(package_name, " needed for this function to work. Please install it via install.packages('", package_name, "')"),
            call. = FALSE)
    }
}
