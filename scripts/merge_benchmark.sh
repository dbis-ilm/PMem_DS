#!/bin/bash

### LOCATIONS ###
REPO_ROOT=$PWD/..
BUILD_DIR=$REPO_ROOT/build
DATA="tree_benchM.data"
REPS=5
OUTPUT_FILE=$PWD/results/mergeFlush.csv

### Create header ###
if [ ! -s $OUTPUT_FILE ]; then
  echo "tree,tblsize,lsize,fillratio,time,writes,pmmwrites" >> $OUTPUT_FILE
fi

### CUSTOMIZABLE PARAMETERS ###
bsize=512
depth=0
LEAF_SIZES=( 256 512 1024 2048 4096)
if [ $# != 1 ]; then
  TREE="UnsortedPBP" #FP/PBP/UnsortedPBP/wBP
  echo "Usage: $0 [<tree-prefix>]"
else
  TREE=$1
fi

### Do not change anything following here!!! ###

### needs manual adaption ###
fillratio=0.5

### adapting Tree usage ###
TREE_BASE=PBP
if [[ "$TREE" =~ ^(wBP|wHBP)$ ]]; then
  TREE_BASE=wBP
elif [[ "$TREE" =~ ^(FP)$ ]]; then
  TREE_BASE=FP
elif [[ "$TREE" =~ ^(BitPBP|BitHPBP)$ ]]; then
  TREE_BASE=BitPBP
fi
TREE_BASE=wBP
sed -i'' -e 's/\(.*BRANCH_SIZE = \)\([0-9]\+\)\(.*\)/\1'"$bsize"'\3/' $REPO_ROOT/bench/trees/common.hpp
sed -i'' -e 's/\(.*DEPTH = \)\([0-9]\+\)\(.*\)/\1'"$depth"'\3/' $REPO_ROOT/bench/trees/common.hpp
sed -i'' -e 's/\(.*\"\).*\(Tree.hpp\"\)/\1'"$TREE"'\2/' $REPO_ROOT/bench/trees/common.hpp #include
sed -i'' -e 's/\(.*LEAFKEYS = getLeafKeys\).*\(Tree.*\)/\1'"$TREE_BASE"'\2/' $REPO_ROOT/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*BRANCHKEYS = getBranchKeys\).*\(Tree.*\)/\1'"$TREE_BASE"'\2/' $REPO_ROOT/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*TreeType = \).*\(Tree.*\)/\1'"$TREE"'\2/' $REPO_ROOT/bench/trees/common.hpp #type
sed -i'' -e 's/\(.*gPmemPath + \"\).*\(\";\)/\1'"$DATA"'\2/' $REPO_ROOT/bench/trees/common.hpp #path

echo -n "Running benchmarks for ${Tree}Tree..."
for lsize in "${LEAF_SIZES[@]}"
do
  echo -n "$lsize "
  sed -i'' -e 's/\(.*LEAF_SIZE = \)\([0-9]\+\)\(.*\)/\1'"$lsize"'\3/' $REPO_ROOT/bench/trees/common.hpp
  pushd $BUILD_DIR > /dev/null
  make tree_merge> /dev/null
  for r in {1..10}
  do
    outLength=$(($REPS + 7))
    OUTPUT="$(sh -c 'bench/tree_merge --benchmark_repetitions='"$REPS"' --benchmark_format=csv' 2> /dev/null | tail -$outLength)"
    writes="$(echo "$OUTPUT" | head -1 | cut -d ':' -f2)"
    pmmwrites="$(echo "$OUTPUT" | head -2 | tail -1| cut -d ':' -f2)"
    elements="$(echo "$OUTPUT" | head -3 | tail -1 | cut -d ':' -f2)"
    time="$(echo "$OUTPUT" | tail -3 | head -1 | cut -d ',' -f3)"
    echo "${TREE}Tree,$elements,$lsize,$fillratio,$time,$writes,$pmmwrites" >> $OUTPUT_FILE
  done
  popd > /dev/null
done
echo -e "\nFinished. Results in $OUTPUT_FILE"
