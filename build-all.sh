#!/bin/bash
# Bray-Core 全平台构建脚本

set -e

echo "=== Bray-Core 全平台构建 ==="
echo ""

# 获取 commit ID
COMMID=$(git describe --always --dirty 2>/dev/null || echo "dev")
echo "构建 commit: ${COMMID}"
echo ""

# 优化级别说明:
# GOAMD64=v3                    - 启用 AVX2/BMI2/FMA/AES-NI 等现代指令集
# -gcflags=all=-l=4             - 最大内联级别 (4层嵌套内联)
# -gcflags=all=-B=0             - 禁用边界检查 (需确保代码安全)
# -gcflags=all=-d=checkptr=0    - 禁用 unsafe.Pointer 转换检查
# -gcflags=all=-d=disablenil    - 禁用 nil 检查
# -gcflags=all=-d=compressinstructions - 压缩指令 (减少代码体积)
# -gcflags=all=-d=mergelocals   - 合并不冲突的局部变量栈槽
# -gcflags=all=-d=zerocopy      - 启用零拷贝 string->[]byte 转换
# -gcflags=-spectre=all         - 禁用 Spectre 缓解措施 (提升性能)
# -ldflags -s -w                - 剥离符号表和调试信息
# -ldflags -compressdwarf       - 压缩 DWARF 调试信息
# -ldflags -linkmode=internal   - 使用内部链接器 (更快)
# -trimpath                     - 移除编译路径信息
# -buildvcs=false               - 跳过 VCS 信息嵌入

GCFLAGS="all=-l=4 -B=0 -d=checkptr=0,disablenil,compressinstructions,mergelocals,zerocopy"

# 清理旧构建
echo "[1/4] 清理旧构建..."
rm -f bray-core-linux-amd64 bray-core-windows-amd64.exe

# Linux AMD64
echo ""
echo "[2/4] 构建 Linux AMD64..."
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOAMD64=v3 go build \
    -o bray-core-linux-amd64 \
    -trimpath \
    -buildvcs=false \
    -gcflags="$GCFLAGS" \
    -ldflags="-X github.com/xtls/xray-core/core.build=${COMMID} -s -w -buildid= -compressdwarf -linkmode=internal" \
    ./main
echo "  完成: $(ls -lh bray-core-linux-amd64 | awk '{print $5}')"

# Windows AMD64
echo ""
echo "[3/4] 构建 Windows AMD64..."
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 GOAMD64=v3 go build \
    -o bray-core-windows-amd64.exe \
    -trimpath \
    -buildvcs=false \
    -gcflags="$GCFLAGS" \
    -ldflags="-X github.com/xtls/xray-core/core.build=${COMMID} -s -w -buildid= -H windowsgui -compressdwarf -linkmode=internal" \
    ./main
echo "  完成: $(ls -lh bray-core-windows-amd64.exe | awk '{print $5}')"

# 结果汇总
echo ""
echo "[4/4] 构建完成!"
echo ""
echo "=== 输出文件 ==="
ls -lh bray-core-linux-amd64 bray-core-windows-amd64.exe
echo ""
echo "=== 构建信息 ==="
echo "构建 commit: ${COMMID}"
echo "REALITY 版本: $(git -C REALITY log --oneline -1 2>/dev/null || echo 'N/A')"
