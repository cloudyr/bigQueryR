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
  ncol <- 0
  col_name <- c()
  for (i in seq_along(schema$name)){
    if(tolower(schema[i,]$type) != "record"){
      ncol <- ncol + 1
      col_name <- c(col_name,schema[i,]$name)
    }else{
      ncol <- ncol + nrow(schema[i,]$fields[[1]])
      col_name <- c(col_name,paste0(schema[i,]$name,".",paste0(schema[i,]$fields[[1]]$name)))
    }
  }
  
  data_f <- as.data.frame(matrix(unlist(unlist(x$rows)), 
                                 ncol = ncol,
                                 byrow = TRUE),
                          stringsAsFactors = FALSE)
  
  types <- tolower(schema$type)
  
  # /**
  #  * This function parse schema and convert a dataframe according to schema and types
  #  * Defined in converter variable
  #  * @param  list schema the schema to use
  #  * @param  list df     the dataframe to apply the schema
  #  * @return list        the converted dataframe
  #  */
  converter_funcs <- function(schema,df){
    data_f <- df
    types <- tolower(schema$type) # Convert types to a lower case to match it against converter values.
    # Loop through all our types which are order the same way as the df
    for (k in seq_along(types)){
      type <- types[k]
      # Base case for the recursion. If the type is not a record type (column of column)
      if(type != "record"){
        # The convert the df at the same position as the type
        # E.g type = string int string then df[1] will be string, df[2] int etc...
        data_f[,k] <- converter[[type]](data_f[,k]) #this returns the function at the position type in converter variable
      }else{
        # If it's a RECORD type then.
        # First, if we have a RECORD type named "C" and columns of "C" are "Bars1" of type [int] and "Bars2" of type [str]
        # Then the df will be C.bars1,C.bars2 at position k to k+2
        # Because records are stored by the privous lines of code in a flatten way
        # [A,B,[C.Bars1,C.Bars2]] will be [A,B,C.Bars1,C.Bars2] where A and B are normal columns
        # See line 26 col_name <- c(...
        # So in this bit of code, we extract the subdataframe ranging from k to k+n-1 where n is the number of rows in the sub schema of the RECORD C
        # and we apply the same function to it's subset from k to k+n-1
        sub_schema <- schema[k,]$fields[[1]]
        n <- nrow(sub_schema)
        data_f[,k:(k+n-1)] <- converter_funcs(sub_schema,data_f[,k:(k+n-1)])
      }
    }
    return(data_f)
  }
  
  # converter_funcs <- converter[types]
  data_f <- converter_funcs(schema,data_f)
  
  names(data_f) <- col_name
  
  out <- data_f
  
  out <- as.data.frame(out, stringsAsFactors = FALSE)
  attr(out, "jobReference") <- x$jobReference
  attr(out, "pageToken") <- x$pageToken
  
  out
    
    
}
