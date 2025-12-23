import os
import subprocess
import sys
import glob

def find_executable(build_dir="build", exe_name="test_join_datasource_modes"):
    """在 build 目录下递归查找可执行文件"""
    print(f"正在 {build_dir} 目录下查找 {exe_name} ...")
    for root, dirs, files in os.walk(build_dir):
        if exe_name in files:
            path = os.path.join(root, exe_name)
            # 检查是否有执行权限
            if os.access(path, os.X_OK):
                return path
    return None

def run_tests():
    # 1. 查找可执行文件
    exe_path = find_executable()
    
    if not exe_path:
        # 尝试硬编码路径作为备选
        fallback = "./build/bin/test_join_datasource_modes"
        if os.path.exists(fallback):
            exe_path = fallback
        else:
            print("❌ 错误：在 build 目录下找不到可执行文件 test_join_datasource_modes")
            print("请先执行编译：cmake --build build --target test_join_datasource_modes -j $(nproc)")
            sys.exit(1)

    print(f"✅ 找到可执行文件：{exe_path}")

    # 2. 定义输出文件
    log_filename = "faiss_test_debug.log"
    
    # 3. 构造命令
    # 注意：这里使用了 *faiss* 过滤器，且忽略大小写匹配（如果 gtest 版本支持，否则依赖前面的配置）
    # 这里的 filter 必须匹配你在 toml 文件中生成的 Test Case 名称
    gtest_filter = "*faiss*" 
    cmd = [exe_path, f"--gtest_filter={gtest_filter}"]

    print(f"🚀 开始运行测试...")
    print(f"   命令：{' '.join(cmd)}")
    print(f"   日志将写入：{log_filename}")
    print("-" * 50)

    # 4. 执行并流式写入日志
    with open(log_filename, "w", encoding="utf-8") as log_file:
        try:
            # 启动进程，合并 stdout 和 stderr
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1  # 行缓冲
            )

            # 实时读取输出
            for line in process.stdout:
                # 写入文件
                log_file.write(line)
                
                # 关键信息打印到控制台，防止用户觉得卡死，但过滤掉大量 debug 信息
                strip_line = line.strip()
                if any(k in strip_line for k in ["[ RUN ]", "[ PASSED ]", "[ FAILED ]", "Segmentation fault", "Error", "Recall"]):
                    print(strip_line)
            
            process.wait()
            
            print("-" * 50)
            if process.returncode == 0:
                print(f"✅ 测试执行完毕 (Exit Code: 0)")
            else:
                print(f"⚠️ 测试执行失败 (Exit Code: {process.returncode})")
                print("这通常意味着出现了 Segfault 或者断言失败。")

        except KeyboardInterrupt:
            print("\n🛑 用户中断测试")
        except Exception as e:
            print(f"\n❌ 脚本运行出错: {e}")

    print(f"\n📄 完整日志已保存至: {os.path.abspath(log_filename)}")
    print("请上传该文件，或查看其中包含 'FAILED' 或 'Recall' 的部分。")

if __name__ == "__main__":
    # 确保在项目根目录运行
    if not os.path.exists("config"):
        print("⚠️ 警告：未检测到 config 目录，请确保你在项目根目录 (sageFlow) 下运行此脚本。")
    run_tests()