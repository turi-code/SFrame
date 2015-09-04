## RUnit for unit test in R package
## lazyeval for function argumen parsing, especially for group_by function
## roxygen2 to generate the documents in R package 

##Make sure we install the right version of Rcpp
if (!"Rcpp"%in%installed.packages()[,'Package'] || installed.packages()["Rcpp",'Version'] != "0.11.6") {
  packageurl <- "http://cran.r-project.org/src/contrib/Archive/Rcpp/Rcpp_0.11.6.tar.gz"
  install.packages(packageurl, repos=NULL, type="source")

  packageurl <- "http://cran.r-project.org/src/contrib/RInside_0.2.13.tar.gz"
  install.packages(packageurl, repos=NULL, type="source")

  packageurl <- "http://cran.r-project.org/src/contrib/inline_0.3.14.tar.gz"
  install.packages(packageurl, repos=NULL, type="source")
}

list.of.packages <- c('lazyeval', 'RUnit', 'roxygen2', 'ggplot2', 'knitr'); 

new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,'Package'])];

if(length(new.packages)) install.packages(new.packages, type = 'source', repo = 'http://cran.rstudio.com/')
