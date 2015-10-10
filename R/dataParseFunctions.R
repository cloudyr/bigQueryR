#' Parse table data
#' 
#' @keywords internal
#' 
#' 
parse_bqr_query <- function(x){
  
  converter <- list(
    integer = as.integer,
    float = as.double,
    boolean = as.logical,
    string = identity,
    timestamp = function(x) as.POSIXct(as.integer(x), origin = "1970-01-01", tz = "UTC")
  )

  schema <- x$schema$fields
  data <- Reduce(rbind, lapply(x$rows$f, function(x) x$v))
  
  types <- tolower(schema$type)
  
  out <- vector("list", length(types))
  for(i in seq_along(types)){
    out[[i]] <- converter[[types[i]]](data[,i])
  }
  names(out) <- schema$name
  
  as.data.frame(out, stringsAsFactors = FALSE)
}
