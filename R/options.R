.onLoad <- function(libname, pkgname) {
  
  op <- options()
  op.bigQueryR <- list(
    bigQueryR.client_id = "68483650948-28g1na33slr3bt8rk7ikeog5ur19ldq6.apps.googleusercontent.com",
    bigQueryR.client_secret = "f0npd8zUhmqf8IqrIypBs6Cy",
    bigQueryR.webapp.client_id = "68483650948-sufabj4nq9h1hjofp03hcjhk4af93080.apps.googleusercontent.com",
    bigQueryR.webapp.client_secret = "0tWYjliwXD32XhvDJHTl4NgN",
    bigQueryR.scope = c("https://www.googleapis.com/auth/bigquery",
                        "https://www.googleapis.com/auth/devstorage.full_control",
                        "https://www.googleapis.com/auth/cloud-platform")
  )
  toset <- !(names(op.bigQueryR) %in% names(op))
  if(any(toset)) options(op.bigQueryR[toset])
  
  invisible()
  
}