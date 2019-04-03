.onLoad <- function(libname, pkgname) {
  
  op <- options()
  op.bigQueryR <- list(
    ## default Google project
    googleAuthR.client_id = "68483650948-28g1na33slr3bt8rk7ikeog5ur19ldq6.apps.googleusercontent.com",
    googleAuthR.client_secret = "f0npd8zUhmqf8IqrIypBs6Cy ",
    googleAuthR.webapp.client_id = "68483650948-sufabj4nq9h1hjofp03hcjhk4af93080.apps.googleusercontent.com",
    googleAuthR.webapp.client_secret = "0tWYjliwXD32XhvDJHTl4NgN ",
    googleAuthR.scopes.selected = c("https://www.googleapis.com/auth/cloud-platform"),
    googleAuthR.batch_endpoint = "https://www.googleapis.com/batch/bigquery/v2"
  )
  
  options(googleAuthR.httr_oauth_cache = "bq.oauth")
  
  toset <- !(names(op.bigQueryR) %in% names(op))
  
  if(any(toset)) options(op.bigQueryR[toset])
  
  invisible()
  
}


.onAttach <- function(libname, pkgname){
  
  attempt <- try(googleAuthR::gar_attach_auto_auth("https://www.googleapis.com/auth/cloud-platform",
                                                   environment_var = "BQ_AUTH_FILE"))
  
  if(inherits(attempt, "try-error")){
    warning("Problem using auto-authentication when loading from BQ_AUTH_FILE.")
  }
  
  if(Sys.getenv("BQ_CLIENT_ID") != ""){
    options(googleAuthR.client_id = Sys.getenv("BQ_CLIENT_ID"))
  }
  
  if(Sys.getenv("BQ_CLIENT_SECRET") != ""){
    options(googleAuthR.client_secret = Sys.getenv("BQ_CLIENT_SECRET"))
  }
  
  if(Sys.getenv("BQ_WEB_CLIENT_ID") != ""){
    options(googleAuthR.webapp.client_id = Sys.getenv("BQ_WEB_CLIENT_ID"))
  }
  
  if(Sys.getenv("BQ_WEB_CLIENT_SECRET") != ""){
    options(googleAuthR.webapp.client_id = Sys.getenv("BQ_WEB_CLIENT_SECRET"))
  }
  
  if(Sys.getenv("BQ_DEFAULT_PROJECT_ID") != ""){
    .bqr_env$project <- Sys.getenv("BQ_DEFAULT_PROJECT_ID")
    packageStartupMessage("Set default project to '", Sys.getenv("BQ_DEFAULT_PROJECT_ID"),"'")
  }
  
  if(Sys.getenv("BQ_DEFAULT_DATASET") != ""){
    .bqr_env$dataset <- Sys.getenv("BQ_DEFAULT_DATASET")
    packageStartupMessage("Set default dataset to '", Sys.getenv("BQ_DEFAULT_DATASET"),"'")
  }
  
  invisible()
  
  }
