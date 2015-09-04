project(metric)

subdirs(mongoose)

make_library(metric
  SOURCES
    mongoose/mongoose.cpp
    metrics_server.cpp
    simple_metrics_service.cpp
  REQUIRES
    logger 
)
