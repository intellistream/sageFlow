# SageFlow + Intel VTune 使用指南

> 目标：对 SageFlow 进行 **多线程锁竞争、线程调度、关键函数耗时** 的精准分析。

## 1. 适用场景

- Join 算子关键路径的热点定位
- 锁竞争与等待时间分析（`SharedWindowState` 读写锁）
- 多线程调度开销、并行效率分析

## 2. 安装（Ubuntu/Debian）

### 2.1 通过 APT 安装（推荐）

```bash
# 1. 添加 Intel GPG 密钥
wget -qO- https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB | \
  sudo gpg --dearmor -o /usr/share/keyrings/oneapi-archive-keyring.gpg

# 2. 添加 Intel 仓库
echo "deb [signed-by=/usr/share/keyrings/oneapi-archive-keyring.gpg] https://apt.repos.intel.com/oneapi all main" | \
  sudo tee /etc/apt/sources.list.d/oneAPI.list

# 3. 安装 VTune
sudo apt update
sudo apt install -y intel-oneapi-vtune

# 4. 安装内核头文件（用于硬件采样驱动）
sudo apt install -y linux-headers-$(uname -r)

# 5. 构建采样驱动（可选，物理机需要）
cd /opt/intel/oneapi/vtune/latest/sepdk/src
sudo ./build-driver -ni

# 6. 初始化环境变量
source /opt/intel/oneapi/vtune/latest/env/vars.sh

# 7. 验证安装
vtune --version
```

### 2.2 添加到 bashrc（可选）

```bash
echo 'source /opt/intel/oneapi/vtune/latest/env/vars.sh 2>/dev/null' >> ~/.bashrc
```

## 3. Docker 环境限制

Docker 容器默认限制硬件 PMU 访问：

| 功能 | 可用性 | 说明 |
|------|--------|------|
| **threading** | ✅ 可用 | 用户态线程分析 |
| **memory-consumption** | ✅ 可用 | 内存分析 |
| **hotspots**（硬件采样）| ❌ 不可用 | 需要 `--privileged` |

如需完整功能，使用特权容器：

```bash
docker run --privileged -v /proc:/proc ...
```

## 4. 构建配置

VTune 需要调试符号：

```bash
cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTING=ON
cmake --build build -j $(nproc)
```

## 5. 命令行使用

### 5.1 Threading 分析（推荐）

分析线程并行度、锁等待：

```bash
source /opt/intel/oneapi/vtune/latest/env/vars.sh

# 运行分析
vtune -collect threading \
  -result-dir vtune_results/join_threading \
  -- ./build/bin/test_join_baseline_integration --gtest_filter='*bruteforce*'

# 查看报告
vtune -report summary -r vtune_results/join_threading
vtune -report hotspots -r vtune_results/join_threading
```

### 5.2 Hotspots 分析（需要权限）

```bash
# 物理机或特权容器
vtune -collect hotspots \
  -result-dir vtune_results/join_hotspots \
  -- ./build/bin/test_join_baseline_integration
```

### 5.3 Locks and Waits 分析

```bash
vtune -collect threading \
  -result-dir vtune_results/locks \
  -- ./build/bin/test_join_baseline_integration

# 导出锁等待报告
vtune -report locks-and-waits -r vtune_results/locks -format csv > locks_report.csv
```

## 6. Web UI 使用

### 6.1 启动 Web 服务

```bash
source /opt/intel/oneapi/vtune/latest/env/vars.sh

vtune-backend \
  --web-port 8443 \
  --allow-remote-access \
  --no-https \
  --data-directory /root/sageFlow/vtune_results \
  --enable-server-profiling \
  --log-to-console
```

### 6.2 访问方式

**本地访问**：
```
http://localhost:8443
```

**VS Code Remote 访问**：
1. 在 VS Code 底部找到 **PORTS** 面板
2. 点击 **Forward a Port**，输入 `8443`
3. 浏览器打开 `http://localhost:8443`

**首次访问**：需要设置 passphrase（密码）

### 6.3 Web UI 功能

| 视图 | 用途 |
|------|------|
| **Summary** | 性能概览、CPU 利用率 |
| **Bottom-up** | 按函数查看时间消耗 |
| **Top-down Tree** | 调用栈分析 |
| **Timeline** | 线程时序图 |
| **Locks and Waits** | 锁竞争详情 |

## 7. SageFlow 关键分析点

### 7.1 重点关注函数

- `JoinOperator::apply`
- `BruteForceBaseline::executeEager`
- `SharedWindowState::addRecord` / `getRecords`
- `ConcurrencyManager::insert` / `query`

### 7.2 重点指标

| 指标 | 说明 |
|------|------|
| **Read/Write Lock Wait** | SharedWindowState 锁竞争 |
| **Sleep Time** | 队列等待、Sink 等待 |
| **CPU Utilization** | 并行效率 |
| **Thread Count** | 实际并行度 |

### 7.3 典型分析结果解读

```
Wait Time with poor CPU Utilization: 59.7s
  Read/Write Lock: 59.7s (16,466 次)
```

说明：多线程在竞争 SharedWindowState 的读写锁，并行度越高竞争越严重。

## 8. 常见问题

### 8.1 "No permission to enable CPU_CLK_UNHALTED.THREAD event"

Docker 容器无硬件 PMU 权限，使用 `threading` 分析代替 `hotspots`。

### 8.2 看不到符号

确认使用 `RelWithDebInfo` 构建，或设置符号路径：

```bash
vtune -search-dir /root/sageFlow/build ...
```

### 8.3 重置 Web UI 密码

目前为Wza990408
```bash
vtune-backend --reset-passphrase
```

## 9. 输出归档

建议存放位置：

```
vtune_results/           # VTune 分析结果（已加入 .gitignore）
├── join_threading/
├── join_hotspots/
└── locks/
```

---

## 快速检查清单

- [ ] 构建模式包含符号（`RelWithDebInfo`）
- [ ] 已初始化环境：`source /opt/intel/oneapi/vtune/latest/env/vars.sh`
- [ ] Docker 环境使用 `threading` 分析
- [ ] Web UI 端口 8443 已转发（VS Code Remote）

