FROM fedora:latest
MAINTAINER Philipp Goetze <philipp.goetze@tu-ilmenau.de>

RUN dnf update -y          \
 && dnf install -y         \
        passwd             \
        git                \
        make               \
        cmake              \
        g++                \
        ndctl-devel        \
        daxctl-devel       \
        libpmemobj++-devel \
 && dnf clean all

# Add user and allow sudo
ENV USER user
ENV USERPASS pass
RUN useradd -m $USER \
 && echo $USERPASS | /usr/bin/passwd $USER --stdin
RUN gpasswd wheel -a $USER
USER $USER

# Set some environment variables
ENV PMEM_IS_PMEM_FORCE 1
ENV CC gcc
ENV CXX g++


# Download and prepare project
RUN cd /home/$USER \
 && git clone https://dbgit.prakinf.tu-ilmenau.de/code/nvm-based_data_structures.git \
 && mkdir nvm-based_data_structures/build \
 && cd nvm-based_data_structures/build \
 && cmake ../src
