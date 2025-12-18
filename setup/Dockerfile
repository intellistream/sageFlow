FROM ubuntu:22.04

RUN apt update && apt install -y ca-certificates wget gnupg

RUN sed -i 's|http://archive.ubuntu.com/ubuntu/|https://mirrors.tuna.tsinghua.edu.cn/ubuntu/|g' /etc/apt/sources.list

# Install base dependencies and DiskANN dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libtool \
    pkg-config \
    libaio-dev \
    libgoogle-perftools-dev \
    libunwind-dev \
    libboost-dev \
    libboost-program-options-dev \
    openssh-server \
    gdb \
    gdbserver

# Install Intel MKL from official Intel repository
RUN wget -qO - https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB | apt-key add - && \
    echo "deb https://apt.repos.intel.com/oneapi all main" > /etc/apt/sources.list.d/oneAPI.list && \
    apt-get update && \
    apt-get install -y intel-oneapi-mkl-devel

# Set MKL environment variables
ENV MKLROOT=/opt/intel/oneapi/mkl/latest
ENV LD_LIBRARY_PATH=${MKLROOT}/lib/intel64:${LD_LIBRARY_PATH}
ENV LIBRARY_PATH=${MKLROOT}/lib/intel64:${LIBRARY_PATH}
ENV CPATH=${MKLROOT}/include:${CPATH}
ENV PKG_CONFIG_PATH=${MKLROOT}/lib/pkgconfig:${PKG_CONFIG_PATH}

# Build gperftools from source
RUN git clone https://github.com/gperftools/gperftools.git && \
    cd gperftools && \
    ./autogen.sh && ./configure && make -j && make install && ldconfig && \
    cd .. && rm -rf gperftools

# Setup SSH and directories
RUN mkdir -p /home/dev/sageflow && \
    mkdir /var/run/sshd && \
    echo 'root:root' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config

# Create MKL environment script for all users
RUN echo '# Intel MKL Environment\n\
export MKLROOT=/opt/intel/oneapi/mkl/latest\n\
export LD_LIBRARY_PATH=${MKLROOT}/lib/intel64:${LD_LIBRARY_PATH}\n\
export LIBRARY_PATH=${MKLROOT}/lib/intel64:${LIBRARY_PATH}\n\
export CPATH=${MKLROOT}/include:${CPATH}\n\
export PKG_CONFIG_PATH=${MKLROOT}/lib/pkgconfig:${PKG_CONFIG_PATH}' > /etc/profile.d/mkl.sh

COPY ../../ /home/dev/sageflow/

EXPOSE 22

CMD ["/usr/sbin/sshd", "-D"]