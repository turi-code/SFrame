##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.
##
## methods-sgraph.r ---- R functions related to sgraph

setMethod("show",
          signature(object = "sgraph"),
          function(object) {
            object@backend$show()
          })

#' The number of vertices of a sgraph
#' @param sg a \code{sgraph}
#' @usage num_vertices(sg)
#' @export
#' @examples
#' v <- sframe(vid = c("cat", "dog", "fossa"))
#' e <- sframe(source = c("cat", "dog", "dog"), dest = c("dog", "cat", "fossa"),
#' relationship = c("dislikes", "likes", "likes"))
#' sg <- sgraph(v, e, "vid", "source", "dest")
#' num_vertices(sg)
num_vertices <- function(sg) {
  if (!is.sgraph(sg))
    stop("not a sgraph")
  
  return(sg@backend$num_vertices())
}

#' The number of edges of a sgraph
#' @param sg a \code{sgraph}
#' @usage num_edges(sg)
#' @export
#' @examples
#' v <- sframe(vid = c("cat", "dog", "fossa"))
#' e <- sframe(source = c("cat", "dog", "dog"), dest = c("dog", "cat", "fossa"),
#' relationship = c("dislikes", "likes", "likes"))
#' sg <- sgraph(v, e, "vid", "source", "dest")
#' num_edges(sg)
num_edges <- function(sg) {
  if (!is.sgraph(sg))
    stop("not a sgraph")
  
  return(sg@backend$num_edges())
}

#' A sgraph object or not
#' @param x an object to test
#' @usage is.sgraph(x)
#' @export
is.sgraph <- function(x)
  inherits(x, "sgraph")

#' Construct a sgraph from sframes
#' @param vertices the sframe containing vertices
#' @param edges the sframe containing edges
#' @param vid id for vertices
#' @param src_id id for edge source
#' @param dst_id id for edge destination
#' @return a sgraph
#' @usage sgraph(vertices, edges, "vid", "source", "dest")
#' @export
#' @examples
#' v <- sframe(vid = c("cat", "dog", "fossa"))
#' e <- sframe(source = c("cat", "dog", "dog"), dest = c("dog", "cat", "fossa"),
#' relationship = c("dislikes", "likes", "likes"))
#' sg <- sgraph(v, e, "vid", "source", "dest")
sgraph <-
  function(vertices, edges, vid = "__id", src_id = "__src_id", dst_id = "__dst_id") {
    sgModule <-
      new(gl_sgraph, vertices@backend$get(), edges@backend$get(), vid, src_id, dst_id)
    return(new("sgraph", backend = sgModule, pointer = sgModule$get()))
  }

#' Number of vertices in a sgraph
#' @param sg a sgraph
#' @usage num_vertices(sg)
#' @export
#' @examples 
#' v <- sframe(vid = c("cat", "dog", "fossa"))
#' e <- sframe(source = c("cat", "dog", "dog"), dest = c("dog", "cat", "fossa"),
#' relationship = c("dislikes", "likes", "likes"))
#' sg <- sgraph(v, e, "vid", "source", "dest")
#' num_vertices(sg)
num_vertices <- function(sg) {
  if (!is.sgraph(sg)) {
    stop("this method is only for sgraph")
  }
  return(sg@backend$num_vertices())
}

#' Number of edges in a sgraph
#' @param sg a sgraph
#' @usage num_edges(sg)
#' @export
#' @examples 
#' v <- sframe(vid = c("cat", "dog", "fossa"))
#' e <- sframe(source = c("cat", "dog", "dog"), dest = c("dog", "cat", "fossa"),
#' relationship = c("dislikes", "likes", "likes"))
#' sg <- sgraph(v, e, "vid", "source", "dest")
#' num_edges(sg)
num_edges <- function(sg) {
  if (!is.sgraph(sg)) {
    stop("this method is only for sgraph")
  }
  return(sg@backend$num_edges())
}

#' Edges in a sgraph
#' @param sg a sgraph
#' @return a sframe
#' @usage edges(sg)
#' @export
#' @examples 
#' v <- sframe(vid = c("cat", "dog", "fossa"))
#' e <- sframe(source = c("cat", "dog", "dog"), dest = c("dog", "cat", "fossa"),
#' relationship = c("dislikes", "likes", "likes"))
#' sg <- sgraph(v, e, "vid", "source", "dest")
#' edges(sg)
edges <- function(sg) {
  if (!is.sgraph(sg)) {
    stop("this method is only for sgraph")
  }
  xptr <- sg@backend$get_edges()
  sfModule <- new(gl_sframe, xptr)
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
}

#' Vertices in a sgraph
#' @param sg a sgraph
#' @return a sframe
#' @usage vertices(sg)
#' @export
#' @examples 
#' v <- sframe(vid = c("cat", "dog", "fossa"))
#' e <- sframe(source = c("cat", "dog", "dog"), dest = c("dog", "cat", "fossa"),
#' relationship = c("dislikes", "likes", "likes"))
#' sg <- sgraph(v, e, "vid", "source", "dest")
#' vertices(sg)
vertices <- function(sg) {
  if (!is.sgraph(sg)) {
    stop("this method is only for sgraph")
  }
  xptr <- sg@backend$get_vertices()
  sfModule <- new(gl_sframe, xptr)
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
}

#' Names of both vertex fields and edge fields in a graph.
#' @param sg a sgraph
#' @usage fields(sg)
#' @export
fields <- function(sg) {
  if (!is.sgraph(sg)) {
    stop("this method is only for sgraph")
  }
  return(sg@backend$get_fields())
}

#' Names of vertex fields in a graph.
#' @param sg a sgraph
#' @usage vertex_fields(sg)
#' @export
vertex_fields <- function(sg) {
  if (!is.sgraph(sg)) {
    stop("this method is only for sgraph")
  }
  return(sg@backend$get_vertex_fields())
}

#' Names of edge fields in a graph.
#' @param sg a sgraph
#' @usage edge_fields(sg)
#' @export
edge_fields <- function(sg) {
  if (!is.sgraph(sg)) {
    stop("this method is only for sgraph")
  }
  return(sg@backend$get_edge_fields())
}

#' Add vertices to a sgraph
#' @usage add_vertices(sg, vertices, vid_field)
#' @param sg a sgraph
#' @param vertices a sframe containing vertices
#' @param vid_field vertex field
#' @return a sgraph
#' @examples
#' v <- sframe(vid = c("cat", "dog", "fossa"))
#' e <- sframe(source = c("cat", "dog", "dog"), dest = c("dog", "cat", "fossa"),
#' relationship = c("dislikes", "likes", "likes"))
#' sg <- sgraph(v, e, "vid", "source", "dest")
#' add_vertices(sg, sframe(vid = "horse"), "vid")
#' @export
add_vertices <- function(sg, vertices, vid_field) {
  if (!is.sgraph(sg)) {
    stop("this method is only for sgraph")
  }
  if (!is.sframe(vertices)) {
    stop("vertices should be a sframe")
  }
  xptr <- sg@backend$add_vertices(vertices@backend$get(), vid_field)
  sgModule <- new(gl_sgraph, xptr)
  return(new("sgraph", backend = sgModule, pointer = sgModule$get()))
}

#' Add edges to a sgraph
#' @usage add_edges(sg, edges, src_field, dst_field)
#' @param sg a sgraph
#' @param edges a sframe containing edges
#' @param src_field edge source field
#' @param dst_field edge destination field
#' @return a sgraph
#' @examples
#' v <- sframe(vid = c("cat", "dog", "fossa"))
#' e <- sframe(source = c("cat", "dog", "dog"), dest = c("dog", "cat", "fossa"),
#' relationship = c("dislikes", "likes", "likes"))
#' sg <- sgraph(v, e, "vid", "source", "dest")
#' add_edges(sg, sframe(source = c("horse"), dest = c("tiger"), 
#' relationship = c("dislike")), "source", "dest")
#' @export
add_edges <- function(sg, edges, src_field, dst_field) {
  if (!is.sgraph(sg)) {
    stop("this method is only for sgraph")
  }
  if (!is.sframe(edges)) {
    stop("vertices should be a sframe")
  }
  xptr <- sg@backend$add_edges(edges@backend$get(), src_field, dst_field)
  sgModule <- new(gl_sgraph, xptr)
  return(new("sgraph", backend = sgModule, pointer = sgModule$get()))
}

#' Return a new sgraph with only the selected vertex fields.
#' @details 
#' Other vertex fields are discarded, while fields that do not exist in the
#' sgraph are ignored. Edge fields remain the same in the new graph.
#' "__id" will always be selected.
#' @param sg a sgraph
#' @param fields vertex fields to select
#' @return a sgraph
#' @examples 
#' v <- sframe(vid = c("cat", "dog", "fossa"), age = c(2, 3, 4), height = c(8, 9, 10))
#' e <- sframe(source = c("cat", "dog", "dog"), dest = c("dog", "cat", "fossa"),
#' relationship = c("dislikes", "likes", "likes"))
#' sg <- sgraph(v, e, "vid", "source", "dest")
#' sg
#' select_vertex_fields(sg, "age")
#' @export
select_vertex_fields <- function(sg, fields) {
  if (!is.sgraph(sg)) {
    stop("this method is only for sgraph")
  }
  xptr <- sg@backend$select_vertex_fields(fields)
  sgModule <- new(gl_sgraph, xptr)
  return(new("sgraph", backend = sgModule, pointer = sgModule$get()))
}

#' Return a new sgraph with only the selected edge fields.
#' @details 
#' Other edge fields are discarded, while fields that do not exist in the
#' sgraph are ignored. Vertex fields remain the same in the new graph.
#' "__src_id" and "__dst_id" will always be selected.
#' @param sg a sgraph
#' @param fields edge fields to select
#' @return a sgraph
#' @examples 
#' v <- sframe(vid = c("Alice", "Bob"))
#' e <- sframe(source = c("Alice", "Bob"),  dest = c("Bob", "Alice"), 
#' follows = c(0, 1), likes = c(5, 3))
#' sg <- sgraph(v, e, "vid", "source", "dest")
#' sg
#' select_edge_fields(sg, "follows")
#' @export
select_edge_fields <- function(sg, fields) {
  if (!is.sgraph(sg)) {
    stop("this method is only for sgraph")
  }
  xptr <- sg@backend$select_edge_fields(fields)
  sgModule <- new(gl_sgraph, xptr)
  return(new("sgraph", backend = sgModule, pointer = sgModule$get()))
}

#' Return a new sgraph with only the selected fields (both vertex and edge fields). 
#' @details 
#' Other fields are discarded, while fields that do not exist in the
#' sgraph are ignored. "__id", "__src_id" and "__dst_id" will always be selected.
#' @param sg a sgraph
#' @param fields fields to select
#' @return a sgraph
#' @examples 
#' v <- sframe(vid = c("Alice", "Bob"))
#' e <- sframe(source = c("Alice", "Bob"),  dest = c("Bob", "Alice"), 
#' follows = c(0, 1), likes = c(5, 3))
#' sg <- sgraph(v, e, "vid", "source", "dest")
#' sg
#' select_fields(sg, "follows")
#' @export
select_fields <- function(sg, fields) {
  if (!is.sgraph(sg)) {
    stop("this method is only for sgraph")
  }
  xptr <- sg@backend$select_fields(fields)
  sgModule <- new(gl_sgraph, xptr)
  return(new("sgraph", backend = sgModule, pointer = sgModule$get()))
}