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
  data_f <- Reduce(rbind, lapply(x$rows$f, function(x) x$v))
  if(length(data_f) == 1) 
    warning("Can't parse data page of 1 length, please modify pagesize until this bug is fixed. 
            E.g. LIMIT 101 and maxResults=100, change maxResults to 99.")
  
  types <- tolower(schema$type)
  
  out <- vector("list", length(types))
  for(i in seq_along(types)){
    out[[i]] <- converter[[types[i]]](data_f[,i])
  }
  names(out) <- schema$name
  
  out <- as.data.frame(out, stringsAsFactors = FALSE)
  attr(out, "jobReference") <- x$jobReference
  attr(out, "pageToken") <- x$pageToken
  
  out
    
    
}
