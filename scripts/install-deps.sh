#!/bin/bash
# ============================================================================
# sageFlow 依赖安装脚本
# 支持 DiskANN 等向量索引库的编译和运行
# ============================================================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${BLUE}=========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}=========================================${NC}"
    echo ""
}

print_success() { echo -e "${GREEN}✓ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠ $1${NC}"; }
print_error() { echo -e "${RED}✗ $1${NC}"; }
print_info() { echo -e "${BLUE}→ $1${NC}"; }

# 检测是否为 root
check_root() {
    if [ "$EUID" -ne 0 ]; then
        print_error "请使用 root 权限运行此脚本"
        print_info "使用: sudo $0"
        exit 1
    fi
}

# 检测操作系统
detect_os() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$ID
        VER=$VERSION_ID
    else
        OS="unknown"
    fi
    print_info "检测到操作系统: $OS $VER"
}

# 安装基础依赖
install_base_deps() {
    print_header "步骤 1/3: 安装基础依赖"
    
    apt-get update
    apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        git \
        pkg-config \
        libtool \
        wget \
        gnupg \
        ca-certificates \
        libaio-dev \
        libgoogle-perftools-dev \
        libunwind-dev \
        libboost-dev \
        libboost-program-options-dev
    
    print_success "基础依赖安装完成"
}

# 安装 Intel MKL
install_mkl() {
    print_header "步骤 2/3: 安装 Intel MKL"
    
    # 检查是否已安装
    if [ -d "/opt/intel/oneapi/mkl/latest" ]; then
        print_info "Intel MKL 已安装，跳过"
        return 0
    fi
    
    print_info "添加 Intel 官方仓库..."
    wget -qO - https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB | apt-key add -
    echo "deb https://apt.repos.intel.com/oneapi all main" > /etc/apt/sources.list.d/oneAPI.list
    
    print_info "安装 Intel MKL (这可能需要几分钟)..."
    apt-get update
    apt-get install -y intel-oneapi-mkl-devel
    
    print_success "Intel MKL 安装完成"
}

# 配置环境变量
configure_environment() {
    print_header "步骤 3/3: 配置环境变量"
    
    # 创建系统级环境变量配置
    cat > /etc/profile.d/mkl.sh << 'EOF'
# Intel MKL Environment
export MKLROOT=/opt/intel/oneapi/mkl/latest
export LD_LIBRARY_PATH=${MKLROOT}/lib/intel64:${LD_LIBRARY_PATH}
export LIBRARY_PATH=${MKLROOT}/lib/intel64:${LIBRARY_PATH}
export CPATH=${MKLROOT}/include:${CPATH}
export PKG_CONFIG_PATH=${MKLROOT}/lib/pkgconfig:${PKG_CONFIG_PATH}
EOF

    # 添加到 bash.bashrc 以便非登录 shell 也能加载
    if ! grep -q "source /etc/profile.d/mkl.sh" /etc/bash.bashrc 2>/dev/null; then
        echo 'source /etc/profile.d/mkl.sh 2>/dev/null' >> /etc/bash.bashrc
    fi
    
    # 刷新动态链接库缓存
    ldconfig
    
    print_success "环境变量配置完成"
}

# 验证安装
verify_installation() {
    print_header "验证安装"
    
    source /etc/profile.d/mkl.sh
    
    local all_ok=true
    
    # 检查 MKL
    if [ -f "/opt/intel/oneapi/mkl/latest/include/mkl.h" ]; then
        print_success "MKL 头文件: OK"
    else
        print_error "MKL 头文件: 未找到"
        all_ok=false
    fi
    
    if [ -f "/opt/intel/oneapi/mkl/latest/lib/intel64/libmkl_core.so" ]; then
        print_success "MKL 库文件: OK"
    else
        print_error "MKL 库文件: 未找到"
        all_ok=false
    fi
    
    # 检查其他依赖
    for lib in aio tcmalloc unwind boost_program_options; do
        if ldconfig -p | grep -q "lib${lib}"; then
            print_success "lib${lib}: OK"
        else
            print_warning "lib${lib}: 可能未安装或未在缓存中"
        fi
    done
    
    if [ "$all_ok" = true ]; then
        print_success "所有依赖安装验证通过！"
    else
        print_warning "部分依赖可能存在问题，请检查上述输出"
    fi
}

# 打印使用说明
print_usage() {
    print_header "安装完成"
    
    echo "依赖已安装完成。使用说明："
    echo ""
    echo "1. 重新登录或执行以下命令加载环境变量："
    echo "   source /etc/profile.d/mkl.sh"
    echo ""
    echo "2. 编译 sageFlow："
    echo "   mkdir build && cd build"
    echo "   cmake -DCMAKE_BUILD_TYPE=Release .."
    echo "   make -j\$(nproc)"
    echo ""
    echo "3. 其他用户首次使用需要重新登录终端"
    echo ""
}

# 主函数
main() {
    print_header "sageFlow 依赖安装脚本"
    
    check_root
    detect_os
    
    if [ "$OS" != "ubuntu" ] && [ "$OS" != "debian" ]; then
        print_error "此脚本仅支持 Ubuntu/Debian 系统"
        exit 1
    fi
    
    install_base_deps
    install_mkl
    configure_environment
    verify_installation
    print_usage
}

main "$@"
