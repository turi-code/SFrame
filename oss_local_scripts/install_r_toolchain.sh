#/bin/bash

set -x
set -e 

function download_file {
  # detect wget
  echo "Downloading $2 from $1 ..."
  if [ -z `which wget` ] ; then
    if [ -z `which curl` ] ; then
      echo "Unable to find either curl or wget! Cannot proceed with
            automatic install."
      exit 1
    fi
    curl $1 -o $2
  else
    wget $1 -O $2
  fi
} # end of download file

hasR=0
if [ -e deps/R/bin/R ]; then
        hasR=1
fi

if [ -e deps/R/bin/R.exe ]; then
        hasR=1
fi
echo $hasR
ROOT_DIR=$PWD

if [[ $hasR == 0 ]]; then
        if [[ $OSTYPE == darwin* ]]; then
            download_file https://cran.rstudio.com/src/base/R-3/R-3.2.1.tar.gz deps/R-3.2.1.tar.gz
            cd deps
            tar zxvf R-3.2.1.tar.gz
            rm -rf R
            mv R-3.2.1 R
            cd R
            ./configure --disable-openmp --with-x=no --enable-R-shlib
            make
        elif [[ $OSTYPE == linux* ]]; then
            download_file https://cran.rstudio.com/src/base/R-3/R-3.2.1.tar.gz deps/R-3.2.1.tar.gz
            cd deps
            tar zxvf R-3.2.1.tar.gz
            rm -rf R
            mv R-3.2.1 R
            cd R
            ./configure --disable-openmp --with-x=no --enable-R-shlib
            make
        fi
fi

cd $ROOT_DIR

# set R home directory
export R_HOME_DIR=$ROOT_DIR/deps/R
export R_HOME=$ROOT_DIR/deps/R

# add R in deps into PATH
export PATH=$R_HOME/bin:$PATH

deps/R/bin/Rscript ./oss_local_scripts/r_requirements.r

mkdir -p deps/local/lib
mkdir -p deps/local/include

# copy libR and libRInside
cp deps/R/lib/libR* deps/local/lib
cp deps/R/library/RInside/lib/libRInside.a deps/local/lib

# copy R/Rcpp/RInside headers
cp -R deps/R/include/* deps/local/include
cp -R deps/R/library/Rcpp/include/* deps/local/include
cp -R deps/R/library/RInside/include/* deps/local/include
cp -R deps/R/library/RApiSerialize/include/* deps/local/include
