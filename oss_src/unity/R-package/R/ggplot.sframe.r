##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.
##
## ggplot.sframe.r

#' Create a new ggplot plot from a sframe
#' @name ggplot2
#' @aliases ggplot2.sframe
#' @param data default sframe for plot
#' @param mapping default list of aesthetic mappings (these can be colour,
#'   size, shape, line type -- see individual geom functions for more details)
#' @param ... ignored
#' @param n the largest number of rows used in plotting. A sub-sframe will be sampled 
#'   if original sframe contains more rows
#' @param environment in which evaluation of aesthetics should occur
#' @usage ggplot <- function(data, mapping = aes(), n = 5000, ..., 
#'                        environment = parent.frame())
#' @export
#' @examples
#' \dontrun{
#' qplot(log(carat), log(price), data = sf)
#' }
ggplot.sframe <-
  function(data, mapping = aes(), n = 5000, ..., environment = parent.frame()) {
    if (!missing(mapping) &&
        !inherits(mapping, "uneval"))
      stop("Mapping should be created with aes or aes_string")
    
    if (nrow(data) > n) {
      print(paste(n, "data points have been sampled from original data"))
      per = n / nrow(data)
      df <- as.data.frame(sample_frac(data, per))
    } else {
      df <- as.data.frame(data)
    }
    
    p <- structure(
      list(
        data = df,
        layers = list(),
        scales = ggplot2:::Scales$new(),
        mapping = mapping,
        theme = list(),
        coordinates = coord_cartesian(),
        facet = facet_null(),
        plot_env = environment
      ), class = c("gg", "ggplot")
    )
    
    p$labels <- ggplot2:::make_labels(mapping)
    ggplot2:::set_last_plot(p)
    p
  }