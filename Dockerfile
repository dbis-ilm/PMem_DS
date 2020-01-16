FROM dbisilm/pmdk:latest
MAINTAINER Philipp Goetze <philipp.goetze@tu-ilmenau.de>


# Set default user
USER $USER
WORKDIR /home/$USER


# Download and prepare project
RUN cd /home/$USER \
 && git clone https://dbgit.prakinf.tu-ilmenau.de/code/nvm-based_data_structures.git \
 && mkdir nvm-based_data_structures/build \
 && cd nvm-based_data_structures/build \
 && cmake ..
