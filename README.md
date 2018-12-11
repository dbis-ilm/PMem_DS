NVM-based Data Strucutres
=========================

This is a repository for persistent data structures based on non-volatile memory (NVM).

## Requirements ##
- ndctl/daxctl - [github](https://github.com/pmem/ndctl) or distribution package manager (e.g., Fedora: ```sudo dnf install ndctl-devel daxctl-devel```)


## Build ##
Configurations can be found in [src/CMakeLists.txt](src/CMakeLists.txt)

```sh
mkdir build; cd build
cmake ../src
make -j
```
Tests and benchmarks, if enabled, can be either manually executed or run completely with: ```make test```

## License ##
The structures are licensed under GPLv3.
Please see the file [COPYING](COPYING) for detailed license information.
