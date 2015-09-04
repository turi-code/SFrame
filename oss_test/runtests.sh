#!/bin/bash

function quit_if_bad_retvalue {
  if [ $? -eq 0 ]; then
    echo "PASS"
  else
    echo "FAIL. Program returned with failure"
    exit 1
  fi
}

function test_rpc_prog {
  echo "Testing $1 ..."
  echo "---------$1-------------" >> $stdoutfname
  echo "---------$1-------------" >> $stderrfname 
  mpiexec -n 2 -host $localhostname ./$1  >> $stdoutfname 2>> $stderrfname
  if [ $? -ne 0 ]; then
    echo "FAIL. Program returned with failure"
    exit 1
  fi
  str="mpiexec -n 2 -host $localhostname ./$1 2> /dev/null | grep \"$2\""
  #echo $str
  e=`eval $str`
  if [ -z "$e" ] ; then
    echo "Expected program output not obtained"
    exit 1
  fi
}

stdoutfname=$PWD/stdout.log
stderrfname=$PWD/stderr.log
echo $PWD | grep debug > /dev/null
dbgpath=$?
echo $PWD | grep release > /dev/null
relpath=$?
echo $PWD | grep profile > /dev/null
propath=$?

if [ $dbgpath -eq 1 ]; then
  if [ $relpath -eq 1 ]; then
    if [ $propath -eq 1 ]; then
	echo "This test must be run from either ./release/tests/, ./debug/tests/, or ./profile/tests/ in Graphlab root folder"
        echo "Please compile GraphLab first, using the instructions on http://graphlab.org/download.html and try again from the approprite folder"
        exit 1
    fi 
  fi
fi



rm -f $stdoutfname $stderrfname

if [ $# -eq 0 ]; then

echo "Running Standard unit tests"
echo "==========================="
ctest -O testlog.txt
./anytests
./anytests_loader
# delete extra generated files
rm -f dg*

fi

echo | tee -a $stdoutfname
echo "Running application tests"| tee -a $stdoutfname
echo "========================="| tee -a $stdoutfname
echo "GraphLab collaborative filtering library"| tee -a $stdoutfname
somefailed=0
if [ -f ../demoapps/pmf/pmf ]; then
  pushd . > /dev/null
  cd ../demoapps/pmf
  echo "---------PMF-------------" >> $stdoutfname
  OUTFILE=smalltest.out
  ./pmf --show_version=true
  if [ $? -eq 2 ]; then
    echo "detected Eigen based pmf"| tee -a $stdoutfname
    OUTFILE=smalltest_eigen.out
  else
    echo "detected it++ based pmf"| tee -a $stdoutfname
  fi
  echo "********************TEST1************************" >> $stdoutfname
  ./pmf --unittest 1 --ncpus=1 --debug=true >> $stdoutfname 2>& 1
  if [ $? -eq 0 ]; then
     echo "PASS TEST 1 (Alternating least squares)"| tee -a $stdoutfname
  else
     somefailed=1
     echo "FAIL --unittest=1"| tee -a $stdoutfname
  fi
  echo "********************TEST2************************" >> $stdoutfname
  ./pmf --unittest 71 --ncpus=1 --debug=true >> $stdoutfname 2>& 1
  if [ $? -eq 0 ]; then
     echo "PASS TEST 2 (Lanczos)"| tee -a $stdoutfname
  else
     somefailed=1
     echo "FAIL --unittest=71 (Lanczos)"| tee -a $stdoutfname
  fi
  echo "********************TEST4************************" >> $stdoutfname
  ./pmf --unittest 91 --ncpus=1 --debug=true >> $stdoutfname 2>& 1
  if [ $? -eq 0 ]; then
     echo "PASS TEST 3 (Weighted ALS)"| tee -a $stdoutfname
  else
     somefailed=1
     echo "FAIL --unittest=91 (weighted alternating least squares)"| tee -a $stdoutfname
  fi
  echo "********************TEST5************************" >> $stdoutfname
 ./pmf --unittest 101 --ncpus=1 >> $stdoutfname 2>& 1 
  if [ $? -eq 0 ]; then
     echo "PASS TEST 4 (CoSaMP)"| tee -a $stdoutfname
  else
     echo "FAIL --unittest=101 (CoSaMP)"| tee -a $stdoutfname
     somefailed=1
  fi
  echo "********************TEST6************************" >> $stdoutfname
 ./pmf --unittest 131  >> $stdoutfname 2>& 1 
  if [ $? -eq 0 ]; then
     echo "PASS TEST 5 (SVD)"| tee -a $stdoutfname
  else
     echo "FAIL --unittest=131 (SVD)"| tee -a $stdoutfname
     somefailed=1
  fi
  popd > /dev/null

else
  echo "PMF not found. "| tee -a $stdoutfname
fi
echo



echo "GraphLab clustring library"| tee -a $stdoutfname
if [ -f ../demoapps/clustering/glcluster ]; then
  pushd . > /dev/null
  cd ../demoapps/clustering
  echo "---------CLUSTERING-------------" >> $stdoutfname
  echo "---------CLUSTERING-------------" >> $stderrfname
  echo "********************TEST1************************" >> $stdoutfname
  ./glcluster --unittest 1  $stdoutfname 2>& 1
  if [ $? -eq 0 ]; then
     echo "PASS TEST 1 (Math functions)"| tee -a $stdoutfname
  else
     somefailed=1
     echo "FAIL --unittest=1 (Math functions)"| tee -a $stdoutfname
  fi
  echo "********************TEST2************************" >> $stdoutfname
  ./glcluster --unittest 2 >> $stdoutfname 2>& 1
  if [ $? -eq 0 ]; then
     echo "PASS TEST 2 (Distance functions)"| tee -a $stdoutfname
  else
     somefailed=1
     echo "FAIL --unittest=2 (Distance functions)"| tee -a $stdoutfname
  fi
  echo "********************TEST3************************" >> $stdoutfname
  ./glcluster --unittest 4 >> $stdoutfname 2>& 1
  if [ $? -eq 0 ]; then
     echo "PASS TEST 3 (Floating point math functions)"| tee -a $stdoutfname
  else
     somefailed=1
     echo "FAIL --unittest=3 (Floating point math functions)"| tee -a $stdoutfname
  fi
  popd  > /dev/null
else
  echo "Clustering library not found. "| tee -a $stdoutfname
fi
 
echo | tee -a $stdoutfname
echo "GraphLab Linear Solvers Library"| tee -a $stdoutfname
if [ -f ../demoapps/gabp/gabp ]; then
  pushd . > /dev/null
  cd ../demoapps/gabp
  echo "---------GABP-------------" >> $stdoutfname
  echo "********************TEST1************************" >> $stdoutfname
  ./gabp --unittest=1 --ncpus=1 --debug=true >> $stdoutfname 2>& 1
  if [ $? -eq 0 ]; then
     echo "PASS TEST 1 (GaBP non-square)"| tee -a $stdoutfname
  else
     somefailed=1
     echo "FAIL --unittest=1 (GabP non-square)"| tee -a $stdoutfname
  fi
  echo "********************TEST2************************" >> $stdoutfname
  ./gabp --unittest=2 --ncpus=1 --debug=true >> $stdoutfname 2>& 1
  if [ $? -eq 0 ]; then
     echo "PASS TEST 2 (GaBP square)"| tee -a $stdoutfname
  else
     somefailed=1
     echo "FAIL --unittest 2 (GaBP square)" | tee -a $stdoutfname
  fi
  echo "********************TEST3************************" >> $stdoutfname
  ./gabp --unittest=3 --ncpus=1 --debug=true >> $stdoutfname 2>& 1
  if [ $? -eq 0 ]; then
     echo "PASS TEST 3 (Jacobi)"| tee -a $stdoutfname
  else
     somefailed=1
     echo "FAIL --unittest=3 (Jacobi)"| tee -a $stdoutfname
  fi
  echo "********************TEST4************************" >> $stdoutfname
 ./gabp --unittest=4 --ncpus=1 >> $stdoutfname 2>& 1 
  if [ $? -eq 0 ]; then
     echo "PASS TEST 4 (Conjugate Gradient - square)"| tee -a $stdoutfname
  else
     echo "FAIL --unittest=4 (Conjugate Gradient - square)"| tee -a $stdoutfname
     somefailed=1
  fi
  echo "********************TEST5************************" >> $stdoutfname
 ./gabp --unittest=5  >> $stdoutfname 2>& 1 
  if [ $? -eq 0 ]; then
     echo "PASS TEST 5 (Conjugate Gradient - non square)"| tee -a $stdoutfname
  else
     echo "FAIL --unittest=5 (Conjugate Gradient- non square)"| tee -a $stdoutfname
     somefailed=1
  fi
  echo "********************TEST6************************" >> $stdoutfname
 ./gabp --unittest=51  >> $stdoutfname 2>& 1 
  if [ $? -eq 0 ]; then
     echo "PASS TEST 6 (Conjugate Gradient, matrix market format)"| tee -a $stdoutfname
  else
     echo "FAIL --unittest=6 (Conjugate Gradient- matrix market)"| tee -a $stdoutfname
     somefailed=1
  fi
  echo "********************TEST7************************" >> $stdoutfname
 ./gabp --unittest=21  >> $stdoutfname 2>& 1 
  if [ $? -eq 0 ]; then
     echo "PASS TEST 7 (GaBP, matrix market format, square, regularization)"| tee -a $stdoutfname
  else
     echo "FAIL --unittest=7 (gaBP, matrix market format, square, regularization)"| tee -a $stdoutfname
     somefailed=1
  fi
   echo "********************TEST8************************" >> $stdoutfname
 ./gabp --unittest=22  >> $stdoutfname 2>& 1 
  if [ $? -eq 0 ]; then
     echo "PASS TEST 8 (Jacobi, matrix market format, symmetric)"| tee -a $stdoutfname
  else
     echo "FAIL --unittest=8 (Jacobi, matrix market format, symmetric)"| tee -a $stdoutfname
     somefailed=1
  fi
   echo "********************TEST9************************" >> $stdoutfname
 ./gabp --unittest=23  >> $stdoutfname 2>& 1 
  if [ $? -eq 0 ]; then
     echo "PASS TEST 9 (Conjugate Gradient, matrix market format, symmetric)"| tee -a $stdoutfname
  else
     echo "FAIL --unittest=9 (Conjugate Gradient, matrix market, symmetric)"| tee -a $stdoutfname
     somefailed=1
  fi
    popd > /dev/null
else
  echo "Linear solver library not found. "| tee -a $stdoutfname
fi
echo


  if [ $somefailed == 1 ]; then
     echo "**** FAILURE LOG **************" >> $stdoutfname
     cat $stderrfname >> $stdoutfname
     echo "**** CONFIGURE.DEPS **************" >> $stdoutfname
     cat ../../configure.deps >> $stdoutfname
     echo "**** CONFIG.LOG **************" >> $stdoutfname
     cat ../../config.log >> $stdoutfname
     echo "**** SYSTEM STATS **************" >> $stdoutfname
     echo `date` >> $stdoutfname
     echo `uname -a` >> $stdoutfname
     echo `echo $USER` >> $stdoutfname
     echo "Some of the tests failed".
     echo "Please email stdout.log to danny.bickson@gmail.com"
     echo "Thanks for helping improve GraphLab!"
  fi




if [ -f ../demoapps/demo/demo ]; then
  pushd . > /dev/null
  cd ../demoapps/demo
  echo "Demo..."
  echo "---------demo-------------" >> $stdoutfname
  echo "---------demo-------------" >> $stderrfname
  
  ./demo  >> $stdoutfname 2>> $stderrfname
  quit_if_bad_retvalue
  popd > /dev/null
else
  echo "demo not found. "
fi

echo
echo "RPC Tests"
echo "========="
echo "Testing for availability of an MPI daemon"
localhostname=`hostname`
mpdtrace
if [ $? -eq 0 ]; then
  echo "MPI available"
else
  echo "MPI not available. Distributed/RPC tests not running."
  exit 1
fi



test_rpc_prog rpc_example1 "5 plus 1 is : 6\\|11 plus 1 is : 12"
test_rpc_prog rpc_example2 "hello world!\\|1, 2, 1,"
test_rpc_prog rpc_example3 "1.a = 10\\|10.b = 0\\|string = hello world!"
test_rpc_prog rpc_example4 "1.a = 10\\|10.b = 0\\|string = hello world!"
test_rpc_prog rpc_example5 "1 + 2.000000 = three"
test_rpc_prog rpc_example6 "10\\|15\\|hello world\\|10.5\\|10"
test_rpc_prog rpc_example7 "set from 1\\|set from 1\\|set from 0\\|set from 0\\|set from 1\\|set from 1\\|set from 0\\|set from 0"

echo
echo "Distributed GraphLab Tests"
echo "=========================="

echo "Testing Distributed disk graph construction..."
echo "---------distributed_dg_construction_test-------------" >> $stdoutfname
echo "---------distributed_dg_construction_test-------------" >> $stderrfname 
mpiexec -n 2 -host $localhostname ./distributed_dg_construction_test >> $stdoutfname 2>> $stderrfname
quit_if_bad_retvalue
rm -f dg*

echo "Testing Distributed Graph ..."
echo "---------distributed_graph_test-------------" >> $stdoutfname
echo "---------distributed_graph_test-------------" >> $stderrfname 
./distributed_graph_test -g
mpiexec -n 2 -host $localhostname ./distributed_graph_test -b >> $stdoutfname 2>> $stderrfname
quit_if_bad_retvalue
rm -f dg*
