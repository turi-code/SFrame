##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.
##
## util.r ---- utility functions, should not be exported
## The main pusrpose for these functions is to capture function dependencies recursively
## A very straightforward method is used here, list all functions in the whole namespace
## and then construct a matrix

# a more general * for vector/matrix
"%**%" <- function (x, y) {
  dimnames(x) <- NULL
  dimnames(y) <- NULL
  if (length(dim(x)) == 2 && length(dim(y)) == 2 && dim(x)[2] ==
      1 && dim(y)[1] == 1)
    return(c(x) %o% c(y))
  if ((!is.null(dim(x)) && any(dim(x) == 1)))
    dim(x) <- NULL
  if ((!is.null(dim(y)) && any(dim(y) == 1)))
    dim(y) <- NULL
  if (is.null(dim(x)) && is.null(dim(y))) {
    if (length(x) == length(y))
      x <- x %*% y
    else {
      if ((length(x) != 1) && (length(y) != 1))
        stop("lengths of x (" %&% length(x) %&% ") and y (" %&%
               length(y) %&% ") are incompatible")
      else
        x <- x * y
    }
  }
  else
    x <- x %*% y
  if ((!is.null(dim(x)) && any(dim(x) == 1)))
    dim(x) <- NULL
  x
}

.char.unlist <- function (x) {
  if (!(listable <- is.list(x))) {
    if (isS4(x) && ('.Data' %in% names(getSlots(class(x)))))
      x <- x@.Data
    if (listable <- (!is.atomic(x) && !is.symbol(x)))
      x <- as.list(x)
  }
  
  if (listable)
    unlist(lapply(x, .char.unlist), use.names = FALSE)
  else
    paste(deparse(x), collapse = "\n")
}

.index <- function (lvector)
  seq_along(lvector)[lvector]

"%that.are.in%" <- function(a, b)
  a[a %in% b]

# core function for this purpose, we need to find this function is called by which one
.called.by <- function(fname, can.match, where) {
  where <-
    if (is.environment(where))
      list(where)
  else
    as.list(where)
  which <-
    unlist(lapply(where, exists, x = fname), use.names = FALSE)
  if (!any(which)) {
    f <- if (exists(fname))
      get(fname)
    else
      list()
  }
  else
    f <- get(fname, pos = where[[.index(which)[1]]])
  
  # flist_ as.character( unlist( f[length(f)], use=FALSE))
  flist <- .char.unlist(f)
  if (!length(flist))
    return(numeric(0))
  
  everything <- flist
  everything <- match(everything, can.match, nomatch = 0)
  everything <- everything[everything > 0]
  everything
}

# Macro-like functions
# Write a "child" function that can create and modify variables in its parent directly,
# without using "<<-"
.mlocal <- function(expr) {
  sp <- sys.parent()
  sp.env <- sys.frame(sp)
  # nlocal_ eval( as.name( 'nlocal'), envir=sp.env) # used to work in S but not in R
  nlocal <- get('nlocal', envir = sp.env)
  nlocal.env <- sys.frame(nlocal)
  on.exit({
    # Get rid of temporaries
    remove(
      list = names(params) %that.are.in%
        (.lsall(env = nlocal.env) %except% names(savers)), envir = nlocal.env
    )
    #   Restore things hidden by params
    for (i in names(savers))
      assign(i, savers[[i]], envir = nlocal.env)
  })
  
  eval(expression(on.exit())[[1]], envir = nlocal.env)
  
  params <- formals(sys.function(sp))
  params <- params[names(params) != 'nlocal']
  savers <- names(params)
  
  if (length(params)) {
    names(savers) <- savers
    savers <-
      sapply(savers, exists, envir = nlocal.env, inherits = FALSE)
    savers <- names(savers)[savers]
    if (length(savers)) {
      names(savers) <- savers
      savers <-
        lapply(savers, function(x)
          mget(x, envir = nlocal.env)[[1]])
    }
    # Parameters and temporary working variables:
    for (i in names(params)) {
      if (eval(call('missing', i), envir = sp.env)) {
        if (is.symbol(params[[i]]) &&
            !nzchar(as.character(params[[i]])) &&
            exists(i, envir = nlocal.env, inherits = FALSE))
          remove(list = i, envir = nlocal.env)
        else
          assign(i, params[[i]], envir = nlocal.env)
      }
      
      else
        # CHANGED from: bugs here? doesn't force... should do so or use delayedAssign?
        assign(i, sp.env[[i]], envir = nlocal.env)
    } # else NORMAL case
  } # parameter loop
  
  expr <-
    substitute(repeat {
      assign('answer', expr, envir = env); break
    },
    list(expr = substitute(expr), env = sys.frame(sys.nframe())))
  
  # The business end!
  on.exit.code <- quote(NULL)
  eval(expr, envir = nlocal.env, enclos = sys.frame(sys.nframe()))
  
  eval(on.exit.code, envir = nlocal.env, enclos = sys.frame(sys.nframe()))
  
  if (exists('override.answer', envir = sys.frame(sys.nframe()), inherits =
             FALSE))
    # set by a call to "local.return"
    answer <- override.answer
  if (exists('answer', envir = sys.frame(sys.nframe()), inherits = FALSE))
    answer # else return NULL. Will only happen if user has a "return" call
  # without "local.return"-- bad practice.
}

.fun.web <- function(nlocal = sys.parent())
  .mlocal({
    funs <- unique(c(funs, generics))
    n <- length(funs)
    # get the list of master/slave and store in a matrix
    funmat <-
      matrix(0, n, n, dimnames = list(MASTER = funs, SLAVE = funs))
    master.of <-
      lapply(funs, .called.by, can.match = funs, where = where)
    n.master <- unlist(lapply(master.of, length))
    
    setup <- c(rep(1:length(funs), n.master), unlist(master.of))
    dim(setup) <- c(sum(n.master), 2)
    funmat[setup] <- 1
    diag(funmat) <- 0 # to drop self-references
    
    # Not interested in calls TO generic functions:
    funmat[,generics] <- 0
    
    # check whether any methods of generic functions:
    drop.generics <- funmat[generics,] %**% rep(1, n) == 0
    if (any(drop.generics)) {
      funs <- funs[-match(generics[drop.generics], funs)]
      funmat <- funmat[funs, funs]
      n <- n - sum(drop.generics)
    }
    
    if (!n)
      stop('Nothing there!')
    level <- rep(0, n); names(level) <- funs
    current.level <- 1
    while (any(level == 0)) {
      tops <-
        rep(1, sum(level == 0)) %**% funmat[level == 0, level == 0] == 0
      if (!any(tops))
        # we have to sort out functions that call each other
        tops <- least.mutual.dependency(funmat, funs, level)
      
      level[dimnames(funmat)[[1]] [level == 0] [tops]] <-
        current.level
      current.level <- current.level + 1
    }
  })

# keep elements if it does not satisfy the condition
"%except%" <- function (vector, condition)
  vector[match(vector, condition, 0) == 0]

# list all functions in this environment
.lsall <- function(...) {
  mc <- match.call(expand.dots = TRUE)
  mc$all.names <- TRUE
  mc[[1]] <- as.name('ls')
  eval(mc, parent.frame())
}

# set names into a vector
.named <- function (x) {
  if (!length(x))
    return(x)
  
  names(x) <- as.character(x)
  x
}

# find functions in pos, which should be an enviroment or can be converted into an enviroment
# as.environment(1) is just the R_GlobalEnv
.find.funs <-
  function(pos = 1, ..., exclude.mcache = TRUE, mode = 'function') {
    findo <- function(pos2) {
      o <- .named(.lsall(pos = pos2, ...))
      
      if (!length(o))
        return(character(0))
      # keep if exists
      keep <-
        sapply(o, exists, where = pos2, mode = mode, inherits = FALSE)
      
      if (!any(keep))
        return(character(0))
      
      names(o) <- NULL
      o[keep]
    }
    
    # make sure we are finding functions in R environments
    if (is.environment(pos))
      pos <- list(pos)
    else
      pos <- lapply(pos, as.environment)
    unlist(lapply(pos, findo), use.names = FALSE)
  }

# given the list of functions, it will return the full list of function dependecies
# generics is the list of functions we do not want to see in the matrix
.funlst <- function(funlst1, generics = c('c','print','plot')) {
  # as.environment(1) is R_GlobalEnv
  where <- as.environment(1)
  pkgs <- names(sessionInfo()$otherPkgs)
  # packages loaded
  if (length(pkgs) > 0) {
    for (i in 1:length(pkgs)) {
      where <- c(where, paste("package:", pkgs[i], sep = ""))
    }
  }
  # all functions, from base/loaded packages and user defined
  funs <- unique(unlist(lapply(where, .find.funs)))
  # get the dependency matrix
  .fun.web()
  # remove the columns and rows with all zeros
  funmat <- funmat[,!apply(funmat == 0, 2, all)]
  funmat <- funmat[!apply(funmat == 0, 1, all),]
  # get dependency
  .get_dep <- function(x) {
    tmp <- funmat[which(rownames(funmat) %in% x),]
    if (length(tmp) == 0) {
      return(NULL)
    } else if (length(dim(tmp)) == 0) {
      tmp <- tmp[tmp != 0]
      return(names(tmp))
    } else {
      tmp <- tmp[,!apply(tmp == 0, 2, all)]
      return(colnames(tmp))
    }
  }
  # get dependency recursively
  funlst2 <- unique(c(funlst1, .get_dep(funlst1)))
  while (length(funlst2) > length(funlst1)) {
    funlst1 <- funlst2
    funlst2 <- unique(c(funlst1, .get_dep(funlst1)))
  }
  return(funlst2)
}

# given a list of functions, return a list of packages and functions
.get_pkg <- function(funlst) {
  pkgs <- names(sessionInfo()$otherPkgs)
  pkg_lst <- c()
  for (i in 1:length(pkgs)) {
    tmp <- ls(paste("package:", pkgs[i], sep = ""))
    if (sum(funlst %in% tmp) > 0) {
      pkg_lst <- c(pkg_lst, pkgs[i])
      funlst <- funlst[!funlst %in% tmp]
    }
  }
  return(list("fun" = funlst, "pkg" = pkg_lst))
}
