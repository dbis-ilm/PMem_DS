include(../../cmake/Testing.cmake.in)

if (BUILD_BENCHMARKS)
  do_bench(trees/tree_get)
  do_bench(trees/tree_traverse)
  # PTABLE
  do_bench(ptable/insert ptable)
  do_bench(ptable/scan ptable)
  do_bench(ptable/point ptable)
endif()
