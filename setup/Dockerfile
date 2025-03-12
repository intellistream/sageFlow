FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libtool \
    openssh-server \
    gdb \
    gdbserver && \
    gdb --version && \
    git clone https://github.com/gperftools/gperftools.git && \
    cd gperftools && \
    ./autogen.sh && ./configure && make -j && make install && ldconfig && \
    mkdir /var/run/sshd && \
    echo 'root:root' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config

EXPOSE 22

CMD ["/usr/sbin/sshd", "-D"]