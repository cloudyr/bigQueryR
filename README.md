# bigQueryR

## Introduction 

This is a package for interacting with [BigQuery](https://cloud.google.com/bigquery/) from within R.

See the [bigQueryR website](http://code.markedmondson.me/bigQueryR) for examples, details and tutorials. 

## Installation ##

[![CRAN](http://www.r-pkg.org/badges/version/bigQueryR)](http://cran.r-project.org/package=bigQueryR)
[![Build Status](https://travis-ci.org/cloudyr/bigQueryR.png?branch=master)](https://travis-ci.org/cloudyr/bigQueryR)
[![codecov.io](http://codecov.io/github/cloudyr/bigQueryR/coverage.svg?branch=master)](http://codecov.io/github/cloudyr/bigQueryR?branch=master)

This package is on CRAN, but to install the latest development version you can install from the cloudyr drat repository:

```R
# latest stable version
install.packages("bigQueryR", repos = c(getOption("repos"), "http://cloudyr.github.io/drat"))
```

Or, to pull a potentially unstable version directly from GitHub:

```R
if(!require("ghit")){
    install.packages("ghit")
}
ghit::install_github("cloudyr/bigQueryR")
```


---
[![cloudyr project logo](http://i.imgur.com/JHS98Y7.png)](https://github.com/cloudyr)
