include(../cmake/Testing.cmake.in)

do_bench(trees/tree_get)
do_bench(trees/tree_traverse)
do_bench(trees/tree_scan)
do_bench(trees/tree_insert)
do_bench(trees/tree_split)
do_bench(trees/tree_erase)
do_bench(trees/tree_balance)
do_bench(trees/tree_merge)
# PTABLE
#do_bench(ptable/insert ptable)
do_bench(ptable/scan ptable)
do_bench(ptable/point ptable)
# PSKIPLISTS
do_bench(pskiplists/pskip_get)
do_bench(pskiplists/pskip_insert)
