.onLoad <- function(libname, pkgname) {
  
  
  
  invisible()
  
}

.onAttach <- function(libname, pkgname){
  
  attempt <- try(googleAuthR::gar_attach_auto_auth("https://www.googleapis.com/auth/bigquery",
                                                   environment_var = "BQ_AUTH_FILE",
                                                   travis_environment_var = "TRAVIS_BQ_AUTH_FILE"))
  
  if(inherits(attempt, "try-error")){
    warning("Problem using auto-authentication when loading from BQ_AUTH_FILE.  
            Run googleAuthR::gar_auth() or googleAuthR::gar_auth_service() instead.")
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
    .bqr_env$project <- Sys.getenv("BQ_DEFAULT_DATASET")
    packageStartupMessage("Set default dataset to '", Sys.getenv("BQ_DEFAULT_DATASET"),"'")
  }
  
  invisible()
  
  }
