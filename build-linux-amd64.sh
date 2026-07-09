#!/bin/bash
# Bray-Core Linux x64 极致优化构建脚本
# 使用本地 REALITY 子模块

set -e

echo "=== Bray-Core Linux AMD64 极致优化构建 ==="
echo "REALITY 子模块 commit: $(git -C REALITY rev-parse --short HEAD 2>/dev/null || echo 'N/A')"
echo ""

# 获取 commit ID
COMMID=$(git describe --always --dirty 2>/dev/null || echo "dev")

# 极致优化编译参数
export GOOS=linux
export GOARCH=amd64
export CGO_ENABLED=0
export GOAMD64=v3        # 目标: AVX2, BMI2, FMA, AES-NI
export GOFLAGS=""         # 清空默认 flags

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

echo "[1/4] 检查依赖..."
go mod download
go mod verify

echo ""
echo "[2/4] 清理旧构建..."
rm -f bray-core-linux-amd64

echo ""
echo "[3/4] 构建 bray-core-linux-amd64 (极致优化)..."
echo "  GOOS=$GOOS GOARCH=$GOARCH GOAMD64=$GOAMD64"
echo "  gcflags: -l=4 -B=0 -d=checkptr=0,disablenil,compressinstructions,mergelocals,zerocopy"
echo "  ldflags: -s -w -buildid= -compressdwarf -linkmode=internal"
echo ""

# 检查是否有 PGO profile
PGO_FLAG=""
if [ -f "cpu.prof" ]; then
    echo "  检测到 cpu.prof, 启用 PGO 优化..."
    PGO_FLAG="-pgo=cpu.prof"
fi

go build \
    -o bray-core-linux-amd64 \
    -trimpath \
    -buildvcs=false \
    -gcflags="all=-l=4 -B=0 -d=checkptr=0,disablenil,compressinstructions,mergelocals,zerocopy" \
    -ldflags="-X github.com/xtls/xray-core/core.build=${COMMID} -s -w -buildid= -compressdwarf -linkmode=internal" \
    -tags="" \
    $PGO_FLAG \
    -v \
    ./main

echo ""
echo "[4/4] 构建完成!"
echo ""

# 显示结果
ls -lh bray-core-linux-amd64
echo ""
echo "=== 构建信息 ==="
file bray-core-linux-amd64 2>/dev/null || echo "file 命令不可用"
echo ""
echo "REALITY 版本: $(git -C REALITY log --oneline -1 2>/dev/null || echo 'N/A')"
echo "构建 commit:  ${COMMID}"
echo "输出文件:     bray-core-linux-amd64"
