project(unity_toolkits)

subdirs(image)

make_empty_library(unity_toolkits
  REQUIRES
    unity_image_export 
    )

