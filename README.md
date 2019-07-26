NVM-based Data Strucutres
=========================
[![pipeline status](https://dbgit.prakinf.tu-ilmenau.de/code/nvm-based_data_structures/badges/master/pipeline.svg)](https://dbgit.prakinf.tu-ilmenau.de/code/nvm-based_data_structures/commits/master)
[![coverage report](https://dbgit.prakinf.tu-ilmenau.de/code/nvm-based_data_structures/badges/master/coverage.svg?job=coverage)](https://dbgit.prakinf.tu-ilmenau.de/code/nvm-based_data_structures/commits/master)

This is a repository for persistent data structures based on non-volatile memory (NVM).
## :sparkles: TODOs: ##
- [x] ~~PBPTree (B⁺-Tree for NVM)~~
- [x] ~~PTable (BDCC + NVM)~~
- [x] ~~Steffen's implementations~~
- [ ] Alexander's implementations
- [ ] Arun's implementations
- [x] ~~Link this repo to PFabric~~
- [x] ~~Data structure installation~~
- [ ] Data structure common benchmark
- [ ] Documentation

## :copyright: License ##
The structures are licensed under GPLv3.
Please see the file [COPYING](COPYING) for detailed license information.

## :heavy_plus_sign: Requirements ##
- C++ Compiler supporting C++17
- PMDK >= 1.5

  | Name | Github | Package |
  | ---- | ------ | ------- |
  | ndctl/daxctl      | [ndctl](https://github.com/pmem/ndctl)                    | ndctl-dev(el), daxctl-dev(el) |
  | PMDK              | [pmdk](https://github.com/pmem/pmdk)                      | libpmemobj-dev(el)           |
  | PMDK C++ bindings |  [libpmemobj-cpp](https://github.com/pmem/libpmemobj-cpp) | libpmemobj++-dev(el)         |
  > **NOTE**: When installing via package manager - libpmemobj++-dev(el) should install the dependencies above, too.
- Emulated NVM device [pmem.io](http://pmem.io/2016/02/22/pm-emulation.html) (optional)


## :gear: Build ##
Configurations can be found in [src/CMakeLists.txt](src/CMakeLists.txt).

```
mkdir build; cd build
cmake ../src
make -j
```
Tests and benchmarks, if enabled, can be either manually executed from the ```build``` folder or run at once with:

```
make test
```
### :whale: Docker ###
There is also the option to create a Docker container.
For this you can either download a pre-built docker image: ```docker pull dbisilm/nvm-based_data_structures``` 
or built it yourself (in project root directory) with: ```docker build .```

After this you can start the container and bash login with e.g.:
```
docker run --rm -i -t dbisilm/nvm-based_data_structures /bin/bash
```


## :books: Documentation ##
:construction:
