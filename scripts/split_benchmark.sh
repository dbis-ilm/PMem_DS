#!/bin/bash

### LOCATIONS ###
REPO_ROOT=$PWD/..
BUILD_DIR=$REPO_ROOT/build
DATA="tree_benchSp.data"
OUTPUT_FILE=$PWD/results/splitTx.csv

### Create header ###
if [ ! -s $OUTPUT_FILE ]; then
  echo "tree,tblsize,lsize,bsize,depth,fillratio,keypos,time,writes" >> $OUTPUT_FILE
fi

### CUSTOMIZABLE PARAMETERS ###
bsize=512
depth=0
LEAF_SIZES=( 256 512 1024 2048 4096 2048 1024 512 256)
if [ $# != 3 ]; then
  TREE="UnsortedPBP" #FP/PBP/UnsortedPBP/wBP
  SUFFIX=""
  echo "Usage: $0 [<tree-prefix> <_copy|_move>]"
else
  TREE=$1
  SUFFIX=$3
fi

### Do not change anything following here!!! ###

### needs manual adaption ###
fillratio=1.0

### adapting Tree usage ###
sed -i'' -e 's/\(.*BRANCH_SIZE = \)\([0-9]\+\)\(.*\)/\1'"$bsize"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
sed -i'' -e 's/\(.*DEPTH = \)\([0-9]\+\)\(.*\)/\1'"$depth"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
sed -i'' -e 's/\(.*\"\).*\(Tree.hpp\"\)/\1'"$TREE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #include
sed -i'' -e 's/\(.*LEAFKEYS = getLeafKeys\).*\(Tree.*\)/\1'"$TREE_BASE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*BRANCHKEYS = getBranchKeys\).*\(Tree.*\)/\1'"$TREE_BASE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*TreeType = \).*\(Tree.*\)/\1'"$TREE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #type
sed -i'' -e 's/\(.*gPmemPath + \"\).*\(\";\)/\1'"$DATA"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #path

echo -n "Running benchmarks for ${Tree}Tree..."
for lsize in "${LEAF_SIZES[@]}"
do
  echo -n "$lsize "
  sed -i'' -e 's/\(.*LEAF_SIZE = \)\([0-9]\+\)\(.*\)/\1'"$lsize"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
  pushd $BUILD_DIR > /dev/null
  make tree_split > /dev/null
  for r in {1..5}
  do
    rm -rf $DATA
    OUTPUT="$(sh -c 'bench/tree_split --benchmark_repetitions=5 --benchmark_format=csv --benchmark_min_time=0.05' 2> /dev/null | tail -11)"
    writes="$(echo "$OUTPUT" | head -1 | cut -d ':' -f2)"
    elements="$(echo "$OUTPUT" | tail -10 | head -1 | cut -d ':' -f2)"
    time="$(echo "$OUTPUT" | tail -3 | head -1 | cut -d ',' -f4)"
    echo "${TREE}Tree$SUFFIX,$elements,$lsize,$bsize,$depth,$fillratio,$(($elements+1)),$time,$writes" >> $OUTPUT_FILE
  done
  popd > /dev/null
done
echo -e "\nFinished. Results in $OUTPUT_FILE"
