## Test environments
* local OS X install, R 3.4.0
* ubuntu 12.04 (on travis-ci), R 3.4.0
* Windows (on rhub) R 3.4.0

## Failed CRAN tests

This is a patch to remove failing tests of 0.3.0 from being ran on CRAN - as they are authenticated API based they fail in CRANs automated testing. 

## R CMD check results

0 errors | 0 warnings | 1 notes

* Possibly mis-spelled words in DESCRIPTION:
  BigQuery (2:30)
  
This is spelt correctly

## Reverse dependencies

googleAnalyticsR is a dependency, that when checked had 0 errors. 

---
