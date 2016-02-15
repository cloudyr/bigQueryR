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
  
  ## when only one row, it makes it 3 obs. of 1 variable instead of 1 obs. of 3
  data_f <- as.data.frame(Reduce(rbind, lapply(x$rows$f, function(x) x$v)), 
                          stringsAsFactors = FALSE)
  
  types <- tolower(schema$type)
  
  out <- vector("list", length(types))
  for(i in seq_along(types)){
    ## this needs to behave when only length 1
    out[[i]] <- converter[[types[i]]](data_f[,i])
  }
  names(out) <- schema$name
  
  out <- as.data.frame(out, stringsAsFactors = FALSE)
  attr(out, "jobReference") <- x$jobReference
  attr(out, "pageToken") <- x$pageToken
  
  out
    
    
}
