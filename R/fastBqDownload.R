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

    if (is.null(result_file_name)) {
        result_file_name <- "fast_bq_download_result"
    } else {
        result_file_name <- gsub("(\\.csv$)|(\\.csv\\.gz$)", "", result_file_name)
    }

    full_result_path <- paste0(target_folder, "/", result_file_name, ".csv.gz")
    if (file.exists(full_result_path) & !refetch) {
        return(data.table::fread(paste("gunzip -c", full_result_path)))
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
            message("\n\nError while saving from Storage to local. Running cleanup of Storage and BigQuery. See original error message below:\n\n")
            message(paste0(e, "\n\n"))
        },
        finally = {if (clean_intermediate_results == TRUE) {
                    cleanIntermediateResults(object_names, gcp_result_name, target_folder)
            }
        }
    )

    output_dt
}


setFastSqlDownloadOptions <- function(global_project_name, global_dataset_name, global_bucket_name) {
    options(googleAuthR.scopes.selected = "https://www.googleapis.com/auth/cloud-platform")

    bigQueryR::bqr_global_project(global_project_name)
    bigQueryR::bqr_global_dataset(global_dataset_name)
    googleCloudStorageR::gcs_global_bucket(global_bucket_name)
}

saveQueryToStorage <- function(query, result_name, useLegacySql){
    time <- Sys.time()
    message("Querying data and saving to BigQuery table")
    query_job <- bigQueryR::bqr_query_asynch(
        query = query,
        useLegacySql = useLegacySql,
        destinationTableId = result_name,
        writeDisposition = "WRITE_TRUNCATE"
    )

    if (suppressMessages(bigQueryR::bqr_wait_for_job(query_job, wait = 2))$status$state == "DONE") {
        time_elapsed <- difftime(Sys.time(), time)
        message(paste("Querying job is finished, time elapsed:", format(time_elapsed,format = "%H:%M:%S")))

        time <- Sys.time()
        message("Writing data to storage")
        extract_job <- suppressMessages(bigQueryR::bqr_extract_data(
                            tableId = result_name,
                            cloudStorageBucket = googleCloudStorageR::gcs_get_global_bucket(),
                            compression = "GZIP",
                            filename = paste0(result_name, "_*.csv.gz")
                ))
    }

    if (suppressMessages(bigQueryR::bqr_wait_for_job(extract_job, wait = 2))$status$state == "DONE") {
        time_elapsed <- difftime(Sys.time(), time)
        message(paste("Writing data to storage is finished, time elapsed:", format(time_elapsed,format = "%H:%M:%S")))
        object_names <- grep(
            result_name,
            googleCloudStorageR::gcs_list_objects()$name,
            value = TRUE
        )
    }
    object_names
}

readFromStorage <- function(object_names, target_folder) {
    createFolder(target_folder)
    chunk_dt_list <- purrr::map(object_names, ~ {
        object <- .
        googleCloudStorageR::gcs_get_object(
            object_name = object,
            saveToDisk = paste0(target_folder, "/", object),
            overwrite = TRUE
        )
        data.table::fread(paste0("gunzip -c ", target_folder, "/", object))
    })
    data.table::rbindlist(chunk_dt_list)
}

unifyLocalChunks <- function(output_dt, object_names, result_file_name, target_folder) {
    if (length(object_names) > 1) {
        data.table::fwrite(output_dt, paste0(target_folder, "/", result_file_name, ".csv"))
        gzipDataAtPath(paste0(target_folder, "/", result_file_name, ".csv"))
    } else{
        file.rename(
            paste0(target_folder, "/", object_names[[1]]),
            paste0(target_folder, "/", result_file_name, ".csv.gz")
        )
    }
}

cleanIntermediateResults <- function(object_names, table_id, target_folder) {
    purrr::walk(
        object_names,
        ~ googleCloudStorageR::gcs_delete_object(object = .x)
    )
    bigQueryR::bqr_delete_table(tableId = table_id)
    if (length(object_names) > 1) {
        purrr::walk(paste0(target_folder, "/", object_names), file.remove)
    }
    message("The queried table on BigQuery and saved file(s) on GoogleCloudStorage have been cleaned up.
        If you want to keep them, use clean_intermediate_results = TRUE.")
}

createFolder <- function(target_folder) {
    if (!dir.exists(target_folder)) {
        dir.create(target_folder, recursive = TRUE)
        message(paste0(target_folder, ' folder does not exist. Creating folder.'))
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
