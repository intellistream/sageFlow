FROM ubuntu:22.04

# 合并多个操作到一个 RUN 指令中
RUN mv /etc/apt/sources.list /etc/apt/sources.list.bak && \
    echo "deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ jammy main restricted universe multiverse" > /etc/apt/sources.list && \
    echo "deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ jammy-updates main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ jammy-backports main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ jammy-security main restricted universe multiverse" >> /etc/apt/sources.list && \
    apt-get update && apt-get install -y \
        build-essential \
        cmake \
        git \
        libtool && \
    git clone https://github.com/gperftools/gperftools.git && \
    cd gperftools && \
    ./autogen.sh && ./configure && make -j && make install && ldconfig 

# 使用 ENTRYPOINT 指令开启终端
ENTRYPOINT ["/bin/bash"]