#!/bin/bash

### LOCATIONS ###
REPO_ROOT=$PWD/..
BUILD_DIR=$REPO_ROOT/build
DATA="tree_benchI.data"
REPS=5
OUTPUT_FILE=$PWD/results/insertAtFlush.csv

### Create header ###
if [ ! -s $OUTPUT_FILE ]; then
  echo "tree,tblsize,lsize,bsize,depth,fillratio,keypos,time,writes,pmmwrites" >> $OUTPUT_FILE
fi

### CUSTOMIZABLE PARAMETERS ###
bsize=512
depth=0
keypos=('first' 'middle' 'last')
LEAF_SIZES=(256 512 1024 2048 4096)
#LEAF_SIZES=($(shuf -e "${LEAF_SIZES[@]}"))
#LEAF_SIZES=($(seq 256 256 4096))
if [ $# != 1 ]; then
  TREE="UnsortedPBP" #FP/PBP/UnsortedPBP/wBP
  echo "Usage: $0 [<tree-prefix>]"
else
  TREE=$1
fi

### Do not change anything following here!!! ###

### needs manual adaption ###
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
#TREE_BASE=wBP
sed -i'' -e 's/\(.*DEPTH = \)\([0-9]\+\)\(.*\)/\1'"$depth"'\3/' $REPO_ROOT/bench/trees/common.hpp
sed -i'' -e 's/\(.*\"\).*\(Tree.hpp\"\)/\1'"$TREE"'\2/' $REPO_ROOT/bench/trees/common.hpp #include
sed -i'' -e 's/\(.*LEAFKEYS = getLeafKeys\).*\(Tree.*\)/\1'"$TREE_BASE"'\2/' $REPO_ROOT/bench/trees/common.hpp #keys
sed -i'' -e 's/\(.*TreeType = \).*\(Tree.*\)/\1'"$TREE"'\2/' $REPO_ROOT/bench/trees/common.hpp #type
sed -i'' -e 's/\(.*gPmemPath + \"\).*\(\";\)/\1'"$DATA"'\2/' $REPO_ROOT/bench/trees/common.hpp #path

echo -n "Running benchmarks for ${TREE}Tree..."
for pos in "${keypos[@]}"
do
  echo -en "\nKey Position: $pos - "
  KEYPOS='(ELEMENTS+1)\/2'
  if [ $pos == "first" ];then
    KEYPOS='1'
  elif [ $pos == "last" ]; then
    KEYPOS='ELEMENTS'
  fi
  sed -i'' -e 's/\(.*KEYPOS = \)\(.\+\)\(;.*\)/\1'"$KEYPOS"'\3/' $REPO_ROOT/bench/trees/common.hpp
  for lsize in "${LEAF_SIZES[@]}"
  do
    echo -n "$lsize "
    sed -i'' -e 's/\(.*LEAF_SIZE = \)\([0-9]\+\)\(.*\)/\1'"$lsize"'\3/' $REPO_ROOT/bench/trees/common.hpp
    pushd $BUILD_DIR > /dev/null
    make tree_insert > /dev/null
    for r in {1..10}
    do
      outLength=$(($REPS + 7))
      OUTPUT="$(sh -c 'bench/tree_insert --benchmark_min_time=0.001 --benchmark_repetitions='"$REPS"' --benchmark_format=csv' 2> /dev/null | tail -$outLength)"
      writes="$(echo "$OUTPUT" | head -1 | cut -d ':' -f2)"
      pmmwrites="$(echo "$OUTPUT" | head -2 | tail -1| cut -d ':' -f2)"
      elements="$(echo "$OUTPUT" | head -3 | tail -1 | cut -d ':' -f2)"
      time="$(echo "$OUTPUT" | tail -3 | head -1 | cut -d ',' -f3)"
      echo "${TREE}Tree,$elements,$lsize,$bsize,$depth,$fillratio,$pos,$time,$writes,$pmmwrites" >> $OUTPUT_FILE
    done
    popd > /dev/null
  done
done
echo -e "\nFinished. Results in $OUTPUT_FILE"
