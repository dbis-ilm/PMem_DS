#!/bin/bash

REPO_ROOT=$PWD
BUILD_DIR=build
DATA=/mnt/mem/test/tree_bench.data
OUTPUT_FILE=$REPO_ROOT/traversF1.csv

lsize=512
fillratio=1.0
keypos="first"
BRANCH_SIZES=( 256 512 1024 2048 4096 )
DEPTHS=(1 2 3 4)
TREE="FP" #FP/PBP/wBP

#adapting Tree usage
sed -i'' -e 's/\(.*\"\).*\(Tree.hpp\"\)/\1'"$TREE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #include
sed -i'' -e 's/\(.*dbis::\).*\(tree;\)/\1'"${TREE,,}"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #namespace
sed -i'' -e 's/\(.*LEAFKEYS = getLeafKeys\).*\(Tree.*\)/\1'"$TREE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*BRANCHKEYS = getBrnachKeys\).*\(Tree.*\)/\1'"$TREE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*TreeType = \).*\(Tree.*\)/\1'"$TREE"'\2/' $REPO_ROOT/src/bench/trees/common.hpp #type

echo -n "Running benchmarks for ${Tree}Tree..."
for depth in "${DEPTHS[@]}"
do
  echo -en "\nTarget depth: $depth - "
  sed -i'' -e 's/\(.*DEPTH = \)\([0-9]\+\)\(.*\)/\1'"$depth"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
  for bsize in "${BRANCH_SIZES[@]}"
  do
    echo -n "$bsize "
    sed -i'' -e 's/\(.*BRANCH_SIZE = \)\([0-9]\+\)\(.*\)/\1'"$bsize"'\3/' $REPO_ROOT/src/bench/trees/common.hpp
    rm -rf $DATA
    pushd $BUILD_DIR > /dev/null
    make tree_traverse > /dev/null
    for r in {1..3}
    do
      OUTPUT="$(sh -c 'bench/tree_traverse --benchmark_format=csv' 2> /dev/null | tail -3)"
      elements="$(echo "$OUTPUT" | head -1 | cut -d ':' -f2)"
      time="$(echo "$OUTPUT" | tail -1 | cut -d ',' -f4)"
      echo "${TREE}Tree,$elements,$lsize,$bsize,$depth,$fillratio,$keypos,$time" >> $OUTPUT_FILE 
    done
    popd > /dev/null
  done
done
echo "\nFinished. Results in $OUTPUT_FILE"
