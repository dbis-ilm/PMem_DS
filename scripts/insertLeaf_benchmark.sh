#!/bin/bash

### LOCATIONS ###
REPO_ROOT=$PWD/..
BUILD_DIR=$REPO_ROOT/build
DATA=/mnt/pmem/test/tree_bench.data
OUTPUT_FILE=$PWD/results/bytesWritten.csv

### Create header ###
if [ ! -s $OUTPUT_FILE ]; then
  echo "tree,tblsize,lsize,bsize,depth,fillratio,keypos,time,writes" >> $OUTPUT_FILE
fi

### CUSTOMIZABLE PARAMETERS ###
bsize=512
depth=0
keypos=('first' 'middle' 'last')
LEAF_SIZES=( 256 512 1024 2048 4096 )
TREE="BitPBP" #FP/PBP/wBP
TREE_BASE="PBP" # for namespace and numKeys determination
SUFFIX="" # in case of binary vs. linear

### Do not change anything following here!!! ###

### needs manual adaption ###
fillratio=1.0

### adapting Tree usage ###
sed -i'' -e 's/\(.*BRANCH_SIZE = \)\([0-9]\+\)\(.*\)/\1'"$bsize"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
sed -i'' -e 's/\(.*DEPTH = \)\([0-9]\+\)\(.*\)/\1'"$depth"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
sed -i'' -e 's/\(.*\"\).*\(Tree.hpp\"\)/\1'"$TREE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #include
sed -i'' -e 's/\(.*dbis::\).*\(tree;\)/\1'"${TREE_BASE,,}"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #namespace
sed -i'' -e 's/\(.*LEAFKEYS = getLeafKeys\).*\(Tree.*\)/\1'"$TREE_BASE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*BRANCHKEYS = getBranchKeys\).*\(Tree.*\)/\1'"$TREE_BASE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*TreeType = \).*\(Tree.*\)/\1'"$TREE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #type

echo -n "Running benchmarks for ${Tree}Tree..."
for pos in "${keypos[@]}"
do
  echo -en "\nKey Position: $pos - "
  KEYPOS='2'
  if [ $pos == "first" ];then
    KEYPOS='ELEMENTS'
  elif [ $pos == "last" ]; then
    KEYPOS='1'
  fi
  sed -i'' -e 's/\(.*KEYPOS = ELEMENTS\/\)\(.\+\)\(;.*\)/\1'"$KEYPOS"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
  for lsize in "${LEAF_SIZES[@]}"
  do
    echo -n "$lsize "
    sed -i'' -e 's/\(.*LEAF_SIZE = \)\([0-9]\+\)\(.*\)/\1'"$lsize"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
    pushd $BUILD_DIR > /dev/null
    make tree_insert > /dev/null
    for r in {1..5}
    do
      rm -rf $DATA
      OUTPUT="$(sh -c 'bench/tree_insert --benchmark_format=csv --benchmark_min_time=0.005' 2> /dev/null | tail -4)"
      writes="$(echo "$OUTPUT" | head -1 | cut -d ':' -f2)"
      elements="$(echo "$OUTPUT" | tail -3 | head -1 | cut -d ':' -f2)"
      time="$(echo "$OUTPUT" | tail -1 | cut -d ',' -f4)"
      echo "${TREE}Tree$SUFFIX,$elements,$lsize,$bsize,$depth,$fillratio,$pos,$time,$writes" >> $OUTPUT_FILE
    done
    popd > /dev/null
  done
done
echo -e "\nFinished. Results in $OUTPUT_FILE"
