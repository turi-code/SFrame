#!/bin/bash

####  Usage ####
# source python_env.sh [debug | release]

# Check build type
if [[ -z $1 ]]; then
        echo "build type must be specified. " 
        echo "Usage: $0 [debug | release ]"
        return
fi
if [[ $PYTHON_ENV_SET == 1 ]]; then
        echo "Previous values set. Unsetting some variables..." 
        if [ ! -z "$OLD_PYTHON_ENV_PATH" ]; then
                export PATH=$OLD_PYTHON_ENV_PATH
                PATH=$OLD_PYTHON_ENV_PATH
        else
                PATH=""
        fi
        if [ ! -z "$OLD_PYTHON_ENV_PYTHONPATH" ]; then
                export PYTHONPATH=$OLD_PYTHON_ENV_PYTHONPATH
                PYTHONPATH=$OLD_PYTHON_ENV_PYTHONPATH
        else
                PYTHONPATH=""
        fi
fi
BUILD_TYPE=$1
BUILD_TYPE=`echo $BUILD_TYPE | tr "[A-Z]" "[a-z]"` # conver to lower case
echo BUILD_TYPE=$BUILD_TYPE

if [[ -z $BASH_SOURCE ]]; then
        SCRIPT_DIR=$( cd "$( dirname $0)" && pwd )
else
        SCRIPT_DIR=$( cd "$( dirname $BASH_SOURCE )" && pwd )
fi
# Graphlab project root
ROOT_DIR=$SCRIPT_DIR/..
PYTHON_SCRIPTS=deps/conda/bin
if [[ $OSTYPE == msys ]]; then
  PYTHON_SCRIPTS=deps/conda/bin/Scripts
fi

# python executable
export PYTHON_EXECUTABLE=$ROOT_DIR/deps/conda/bin/python
export NOSETEST_EXECUTABLE=$ROOT_DIR/$PYTHON_SCRIPTS/nosetests
export PIP_EXECUTABLE=$ROOT_DIR/$PYTHON_SCRIPTS/pip

# environment varialbes for running the test
export GRAPHLAB_ROOT=$ROOT_DIR
export GRAPHLAB_BUILD_ROOT=$ROOT_DIR/$BUILD_TYPE
if [ -z "$PYTHONPATH" ]; then
        export PYTHONPATH="$GRAPHLAB_BUILD_ROOT/oss_src/unity/python"
else
        export PYTHONPATH="$GRAPHLAB_BUILD_ROOT/oss_src/unity/python:$PYTHONPATH"
        export OLD_PYTHON_ENV_PYTHONPATH=$PYTHONPATH
fi
export PYTHONHOME=$ROOT_DIR/deps/conda
if [[ $OSTYPE == msys ]]; then
        export PYTHONHOME="$PYTHONHOME/bin"
fi
export PYTHON_ENV_SET=1
export OLD_PYTHON_ENV_PATH=$PATH

if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
        # s3 credential - dataset user (locked down read-only access to S3)
        export AWS_ACCESS_KEY_ID='AKIAJG2DI6AOIQOJIZYA'
        export AWS_SECRET_ACCESS_KEY='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'
fi

# hadoop path
if [ -e /opt/hadoop-1.2.1/bin ]; then
        export PATH=/opt/hadoop-1.2.1/bin:$PATH
fi

if [[ $OSTYPE == msys ]]; then
  export PATH=$ROOT_DIR/deps/conda/bin:$ROOT_DIR/deps/conda/bin/Scripts:$ROOT_DIR/deps/local/bin:$PATH
else
  export PATH=$ROOT_DIR/deps/conda/bin:$ROOT_DIR/deps/local/bin:$PATH
fi
echo GRAPHLAB_ROOT=$GRAPHLAB_ROOT
echo GRAPHLAB_BUILD_ROOT=$GRAPHLAB_BUILD_ROOT
echo PYTHONPATH=$PYTHONPATH
echo PYTHONHOME=$PYTHONHOME
echo PYTHON_EXECUTABLE=$PYTHON_EXECUTABLE
echo NOSETEST_EXECUTABLE=$NOSETEST_EXECUTABLE
echo PIP_EXECUTABLE=$PIP_EXECUTABLE
