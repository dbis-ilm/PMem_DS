include(../../cmake/Testing.cmake.in)

do_bench(trees/tree_get)
do_bench(trees/tree_traverse)
do_bench(trees/tree_scan)
do_bench(trees/tree_insert)
do_bench(trees/tree_split)
# PTABLE
#do_bench(ptable/insert ptable)
do_bench(ptable/scan ptable)
do_bench(ptable/point ptable)
