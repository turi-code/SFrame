SFrame Package
=========

Introduction
------------
SFrame Package is the open source piece of [GraphLab Create](https://dato.com/products/create/).
 
For more details on the GraphLab Create (including documentation and tutorials) see http://dato.com.

The SFrame Package provides the complete implementation of:
 - SFrame
 - SArray
 - SGraph 
 - SFrame Query Evaluator
 - The C++ SDK surface area (gl_sframe, gl_sarray, gl_sgraph)
 - Sketch Summary
 - Serialization
 - flexible_type (efficient runtime typed object)
 - C++ interprocess communication library (used for C++ <--> Python
     communication)
 - PowerGraph's RPC implementation

License
-------
The SFrame Package is licensed under a BSD license. See [license](LICENSE) file.

Open Source Commitment
----------------------
We will keep the open sourced components up to date with the latest released
version of GraphLab Create by issuing a large "version update" pull request
with every new version of GraphLab Create.

Dependencies
------------
* On OS X: Apple XCode 6 Command Line Tools [Required]
  +  Required for compiling GraphLab Create.

* On Linux: g++ (>= 4.8) or clang (>= 3.4) [Required]
  +  Required for compiling GraphLab Create.

* *nix build tools: patch, make [Required]
   +  Should come with most Mac/Linux systems by default. Recent Ubuntu versions
   will require installing the build-essential package.

* cython [Required]
   +  For compilation of GraphLab Create

* zlib [Required]
   +   Comes with most Mac/Linux systems by default. Recent Ubuntu version will
   require the zlib1g-dev package.

* JDK 6 or greater [Optional]
   + Required for HDFS support 

* Open MPI or MPICH2 [Optional]
   + Required only for the RPC library inherited from PowerGraph

### Satisfying Dependencies on Mac OS X

Install XCode 6 with the command line tools. Then:

    brew install automake
    brew install autoconf
    brew install cmake

### Satisfying Dependencies on Ubuntu

In Ubuntu >= 12.10, you can satisfy the dependencies with the following:

    sudo apt-get update
    sudo apt-get install gcc g++ build-essential libopenmpi-dev default-jdk cmake zlib1g-dev \
        libatlas-base-dev automake autoconf python-dev python-pip
    sudo pip install cython

For Ubuntu versions prior to 12.10, you will need to install a newer version of gcc

    sudo add-apt-repository ppa:ubuntu-toolchain-r/test
    sudo apt-get update
    sudo apt-get install gcc-4.8 g++-4.8 build-essential libopenmpi-dev default-jdk cmake zlib1g-dev
    #redirect g++ to point to g++-4.8
    sudo rm /usr/bin/g++
    sudo ln -s /usr/bin/g++-4.8 /usr/bin/g++
    
Compiling
---------
After you have done a git pull, cd into the SFrame directory and run the configure script:

    cd SFrame
    ./configure

Running configure will create two sub-directories, *release* and *debug*.  Select 
either of these modes/directories and navigate to the *oss_src/unity* subdirectory:

    cd <repo root>/debug/oss_src/unity
   
   or
   
    cd <repo root>/release/oss_src/unity

Running **make** will build the project, according to the mode selected. 

We recommend using make's parallel build feature to accelerate the compilation
process. For instance:

    make -j 4

Will perform up to 4 build tasks in parallel. The number of tasks to run in parallel should
be roughly equal to the number of CPUs you have.

In order to use the dev build that you have just compiled, some environment variables will need to be set.
This can be done by sourcing a script. You'll need to pass the script either "debug" or "release" depending
on the type of build you have compiled:
  
    source <repo root>/oss_local_scripts/python_env.sh [debug | release ]
 
Running Unit Tests
------------------

### Running Python unit tests
There is a script that makes it easy to run the python unit test. You will just need to call it and pass it
your build type (either "debug" or "release).

    <repo root>/oss_local_scripts/run_python_test.sh [debug | release]

### Running C++ Units Tests
There are also C++ tests. To compile and run them, do the following:

    cd <repo root>/[debug | release]/oss_test
    make
    ctest
  
Writing Your Own Apps
---------------------

See: https://github.com/dato-code/GraphLab-Create-SDK
