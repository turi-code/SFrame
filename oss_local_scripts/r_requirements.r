## RUnit for unit test in R package
## lazyeval for function argumen parsing, especially for group_by function
## roxygen2 to generate the documents in R package 

list.of.packages <- c('devtools', 'Rcpp', 'RInside', 'inline', 'lazyeval', 'RUnit', 'roxygen2', 'ggplot2', 'knitr'); 
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,'Package'])];

if(length(new.packages)) install.packages(new.packages, type = 'source', repo = 'http://cran.rstudio.com/')

devtools::install_github("thirdwing/rapiserialize")
