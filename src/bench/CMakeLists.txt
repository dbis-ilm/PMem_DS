include(../../cmake/Testing.cmake.in)

if (BUILD_BENCHMARKS)
  # PTABLE
  do_bench(insert ptable)
  do_bench(scan ptable)
  do_bench(point ptable)
endif()
