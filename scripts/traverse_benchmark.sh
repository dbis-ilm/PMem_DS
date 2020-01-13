#!/bin/bash

### LOCATIONS ###
REPO_ROOT=$PWD/..
BUILD_DIR=$REPO_ROOT/build
DATA="tree_benchT.data"
OUTPUT_FILE=$PWD/results/traverse.csv
REPS=5

### Create header ###
if [ ! -s $OUTPUT_FILE ]; then
    echo "tree,tblsize,lsize,bsize,depth,fillratio,keypos,time" >> $OUTPUT_FILE
fi

### CUSTOMIZABLE PARAMETERS ###
lsize=512
BRANCH_SIZES=(256 512 1024 2048 4096)
DEPTHS=(1 2 3 4 5 6 7)
if [ $# != 2 ]; then
  TREE="UnsortedPBP" #FP/PBP/UnsortedPBP/wBP
  hybrid="false"
  echo "Usage: $0 [<tree-prefix> <is-hybrid>]"
else
  TREE=$1
  hybrid=$2
fi

### Do not change anything following here!!! ###

### needs manual adaption ###
keypos="first"
fillratio=1.0

### adapting Tree usage ###
TREE_BASE=PBP
if [[ "$TREE" =~ ^(wBP|wHBP)$ ]]; then
  TREE_BASE=wBP
elif [[ "$TREE" =~ ^(FP)$ ]]; then
  TREE_BASE=FP
elif [[ "$TREE" =~ ^(BitPBP|BitHPBP)$ ]]; then
  TREE_BASE=BitPBP
fi
sed -i'' -e 's/\(.*LEAF_SIZE = \)\([0-9]\+\)\(.*\)/\1'"$lsize"'\3/' $REPO_ROOT/bench/trees/common.hpp
sed -i'' -E -e 's/(.*HYBRID = )(0|1|false|true)(.*)/\1'"$hybrid"'\3/' $REPO_ROOT/bench/trees/common.hpp
sed -i'' -e 's/\(.*\"\).*\(Tree.hpp\"\)/\1'"$TREE"'\2/' $REPO_ROOT/bench/trees/common.hpp #include
sed -i'' -e 's/\(.*LEAFKEYS = getLeafKeys\).*\(Tree.*\)/\1'"$TREE_BASE"'\2/' $REPO_ROOT/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*BRANCHKEYS = getBranchKeys\).*\(Tree.*\)/\1'"$TREE_BASE"'\2/' $REPO_ROOT/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*TreeType = \).*\(Tree.*\)/\1'"$TREE"'\2/' $REPO_ROOT/bench/trees/common.hpp #type
sed -i'' -e 's/\(.*gPmemPath + \"\).*\(\";\)/\1'"$DATA"'\2/' $REPO_ROOT/bench/trees/common.hpp #path

echo -n "Running benchmarks for ${TREE}Tree..."
for depth in "${DEPTHS[@]}"
do
  echo -en "\nTarget depth: $depth - "
  sed -i'' -e 's/\(.*DEPTH = \)\([0-9]\+\)\(.*\)/\1'"$depth"'\3/' $REPO_ROOT/bench/trees/common.hpp
  for bsize in "${BRANCH_SIZES[@]}"
  do
    echo -n "$bsize "
    sed -i'' -e 's/\(.*BRANCH_SIZE = \)\([0-9]\+\)\(.*\)/\1'"$bsize"'\3/' $REPO_ROOT/bench/trees/common.hpp
    pushd $BUILD_DIR > /dev/null
    make tree_traverse > /dev/null
    for r in {1..5}
    do
      outLength=$(($REPS + 5))
      OUTPUT="$(sh -c 'bench/tree_traverse --benchmark_repetitions='"$REPS"' --benchmark_format=csv' 2> /dev/null | tail -$outLength)"
      elements="$(echo "$OUTPUT" | head -1 | cut -d ':' -f2)"
      time="$(echo "$OUTPUT" | tail -3 | head -1 | cut -d ',' -f4)"
      echo "${TREE}Tree,$elements,$lsize,$bsize,$depth,$fillratio,$keypos,$time" >> $OUTPUT_FILE
    done
    popd > /dev/null
  done
done
echo -e "\nFinished. Results in $OUTPUT_FILE"
