vigns <- tools::pkgVignettes(dir = '.', subdirs=file.path("inst", "doc"), source = TRUE, output = TRUE)
idx <- tools:::.build_vignette_index(vigns)
saveRDS(idx, file = file.path("build", "vignette.rds"))
