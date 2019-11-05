#!/bin/bash

### LOCATIONS ###
REPO_ROOT=$PWD/..
BUILD_DIR=$REPO_ROOT/build
DATA=/mnt/pmem0/test/tree_benchL.data
OUTPUT_FILE=$PWD/results/lookup.csv

### CUSTOMIZABLE PARAMETERS ###
bsize=512
depth=1
keypos=('first' 'middle' 'last')
LEAF_SIZES=( 256 512 1024 2048 4096 )

### Do not change anything following here!!! ###
if [ $# != 2 ]; then
  TREE="UnsortedPBP" #FP/PBP/UnsortedPBP/HPBP/wBP/wHBP
  hybrid="false"
  echo "Usage: $0 [<tree-prefix> <is-hybrid>]"
else
  TREE=$1
  hybrid=$2
fi


### needs manual adaption ###
fillratio=1.0

### adapting Tree usage ###
TREE_BASE=PBP
if [[ "$TREE" =~ ^(wBP|wHBP)$ ]]; then
  TREE_BASE=wBP
elif [[ "$TREE" =~ ^(FP)$ ]]; then
  TREE_BASE=FP
elif [[ "$TREE" =~ ^(BitPBP)$ ]]; then
  TREE_BASE=BitPBP
fi
sed -i'' -e 's/\(.*BRANCH_SIZE = \)\([0-9]\+\)\(.*\)/\1'"$bsize"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
sed -i'' -e 's/\(.*DEPTH = \)\([0-9]\+\)\(.*\)/\1'"$depth"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
sed -i'' -E -e 's/(.*HYBRID = )(0|1|false|true)(.*)/\1'"$hybrid"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
sed -i'' -e 's/\(.*\"\).*\(Tree.hpp\"\)/\1'"$TREE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #include
sed -i'' -e 's/\(.*LEAFKEYS = getLeafKeys\).*\(Tree.*\)/\1'"$TREE_BASE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*BRANCHKEYS = getBranchKeys\).*\(Tree.*\)/\1'"$TREE_BASE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*TreeType = \).*\(Tree.*\)/\1'"$TREE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #type

echo -n "Running benchmarks for ${TREE}Tree..."
for pos in "${keypos[@]}"
do
  echo -en "\nKey Position: $pos - "
  KEYPOS='(LEAFKEYS+1)\/2'
  if [ $pos == "first" ];then
    KEYPOS='1'
  elif [ $pos == "last" ]; then
    KEYPOS='LEAFKEYS'
  fi
  sed -i'' -e 's/\(.*KEYPOS = \)\(.\+\)\(;.*\)/\1'"$KEYPOS"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
  for lsize in "${LEAF_SIZES[@]}"
  do
    echo -en "\nLeaf size: $lsize"
    sed -i'' -e 's/\(.*LEAF_SIZE = \)\([0-9]\+\)\(.*\)/\1'"$lsize"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
    rm -rf $DATA
    pushd $BUILD_DIR > /dev/null
    make tree_get > /dev/null
    for r in {1..5}
    do
      OUTPUT="$(sh -c 'bench/tree_get --benchmark_repetitions=5 --benchmark_format=csv' 2> /dev/null | tail -10)"
      elements="$(echo "$OUTPUT" | head -1 | cut -d ':' -f2)"
      time="$(echo "$OUTPUT" | tail -3 | head -1 | cut -d ',' -f4)"
      echo "${TREE}Tree,$elements,$lsize,$bsize,$depth,$fillratio,$pos,$time" >> $OUTPUT_FILE
    done
    popd > /dev/null
  done
done
echo -e "\nFinished. Results in $OUTPUT_FILE"
