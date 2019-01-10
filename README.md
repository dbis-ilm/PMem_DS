NVM-based Data Strucutres
=========================
This is a repository for persistent data structures based on non-volatile memory (NVM).
## TODOs: ##
- [x] ~~PBPTree (B⁺-Tree for NVM)~~
- [x] ~~PTable (BDCC + NVM)~~
- [x] ~~Steffen's implementations~~
- [ ] Alexander's implementations
- [ ] Arun's implementations
- [x] ~~Link this repo to PFabric~~
- [x] ~~Data structure installation~~
- [ ] Data structure common benchmark
- [ ] Documentation

## License ##
The structures are licensed under GPLv3.
Please see the file [COPYING](COPYING) for detailed license information.

## Requirements ##
- C++ Compiler supporting C++17
- ndctl/daxctl - via [github](https://github.com/pmem/ndctl) or package manager (e.g., Fedora: ```dnf install ndctl-devel daxctl-devel```)
- Emulated NVM device [pmem.io](http://pmem.io/2016/02/22/pm-emulation.html) (optional)


## Build ##
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

## Documentation ##
TODO
