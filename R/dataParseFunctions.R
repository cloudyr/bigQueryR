#' Parse table data
#' 
#' @keywords internal
parse_bqr_query <- function(x){
  
  converter <- list(
    integer = as.integer,
    float = as.double,
    boolean = as.logical,
    string = identity,
    numeric = as.numeric,
    timestamp = function(x) as.POSIXct(as.integer(x), origin = "1970-01-01", tz = "UTC"),
    date = function(x) as.Date(x, format="%Y-%m-%d") #fix for #22 if using schema DATE
  )

  schema <- x$schema$fields
  ## ffs
  data_f <- as.data.frame(matrix(unlist(unlist(x$rows)), 
                                 ncol = length(schema$name),
                                 byrow = TRUE), 
                          stringsAsFactors = FALSE)
  
  types <- tolower(schema$type)
  
  converter_funcs <- converter[types]

  for(i in seq_along(converter_funcs)){
    data_f[,i] <- converter_funcs[[i]](data_f[, i])
  }
  
  names(data_f) <- schema$name
  
  out <- data_f
  
  out <- as.data.frame(out, stringsAsFactors = FALSE)
  attr(out, "jobReference") <- x$jobReference
  attr(out, "pageToken") <- x$pageToken
  
  out
    
    
}
