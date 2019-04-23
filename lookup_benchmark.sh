#!/bin/bash

### LOCATIONS ###
REPO_ROOT=$PWD
BUILD_DIR=build
DATA=/mnt/mem/test/tree_bench.data
OUTPUT_FILE=$REPO_ROOT/results/searchNoOpt.csv

### CUSTOMIZABLE PARAMETERS ###
bsize=512
depth=1
LEAF_SIZES=( 256 512 1024 2048 4096 )
TREE="FP" #FP/PBP/wBP
SUFFIX="" # in case of binary vs. linear

### Do not change anything following here!!! ###

### needs manual adaption ###
fillratio=1.0
keypos="last" #first/middle/last

### adapting Tree usage ###
sed -i'' -e 's/\(.*BRANCH_SIZE = \)\([0-9]\+\)\(.*\)/\1'"$bsize"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
sed -i'' -e 's/\(.*DEPTH = \)\([0-9]\+\)\(.*\)/\1'"$depth"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
sed -i'' -e 's/\(.*\"\).*\(Tree.hpp\"\)/\1'"$TREE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #include
sed -i'' -e 's/\(.*dbis::\).*\(tree;\)/\1'"${TREE,,}"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #namespace
sed -i'' -e 's/\(.*LEAFKEYS = getLeafKeys\).*\(Tree.*\)/\1'"$TREE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*BRANCHKEYS = getBranchKeys\).*\(Tree.*\)/\1'"$TREE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*TreeType = \).*\(Tree.*\)/\1'"$TREE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #type

echo -n "Running benchmarks for ${Tree}Tree..."
for lsize in "${LEAF_SIZES[@]}"
do
  echo -en "\nLeaf size: $lsize"
  sed -i'' -e 's/\(.*LEAF_SIZE = \)\([0-9]\+\)\(.*\)/\1'"$lsize"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
  rm -rf $DATA
  pushd $BUILD_DIR > /dev/null
  make tree_get > /dev/null
  for r in {1..3}
  do
    OUTPUT="$(sh -c 'bench/tree_get --benchmark_format=csv' 2> /dev/null | tail -3)"
    elements="$(echo "$OUTPUT" | head -1 | cut -d ':' -f2)"
    time="$(echo "$OUTPUT" | tail -1 | cut -d ',' -f4)"
    echo "${TREE}Tree$SUFFIX,$elements,$lsize,$bsize,$depth,$fillratio,$keypos,$time" >> $OUTPUT_FILE
  done
  popd > /dev/null
done
echo -e "\nFinished. Results in $OUTPUT_FILE"
