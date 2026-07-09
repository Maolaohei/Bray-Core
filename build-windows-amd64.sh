#!/bin/bash
# Bray-Core Windows x64 极致优化构建脚本
# 使用本地 REALITY 子模块

set -e

echo "=== Bray-Core Windows AMD64 极致优化构建 ==="
echo "REALITY 子模块 commit: $(git -C REALITY rev-parse --short HEAD 2>/dev/null || echo 'N/A')"
echo ""

# 获取 commit ID
COMMID=$(git describe --always --dirty 2>/dev/null || echo "dev")

# 极致优化编译参数
export GOOS=windows
export GOARCH=amd64
export CGO_ENABLED=0
export GOAMD64=v3        # 目标: AVX2, BMI2, FMA, AES-NI
export GOFLAGS=""         # 清空默认 flags

echo "[1/5] 检查依赖..."
go mod download
go mod verify

echo ""
echo "[2/5] 清理旧构建..."
rm -f bray-core-windows-amd64.exe

echo ""
echo "[3/5] 构建 bray-core-windows-amd64.exe (极致优化)..."
echo "  GOOS=$GOOS GOARCH=$GOARCH GOAMD64=$GOAMD64"
echo "  gcflags: -l=4 -B=0 -d=checkptr=0,disablenil,compressinstructions,mergelocals,zerocopy"
echo "  ldflags: -s -w -buildid= -H windowsgui -compressdwarf -linkmode=internal"
echo ""

# 检查是否有 PGO profile
PGO_FLAG=""
if [ -f "cpu.prof" ]; then
    echo "  检测到 cpu.prof, 启用 PGO 优化..."
    PGO_FLAG="-pgo=cpu.prof"
fi

go build \
    -o bray-core-windows-amd64.exe \
    -trimpath \
    -buildvcs=false \
    -gcflags="all=-l=4 -B=0 -d=checkptr=0,disablenil,compressinstructions,mergelocals,zerocopy" \
    -ldflags="-X github.com/xtls/xray-core/core.build=${COMMID} -s -w -buildid= -H windowsgui -compressdwarf -linkmode=internal" \
    -tags="" \
    $PGO_FLAG \
    -v \
    ./main

echo ""
echo "[4/5] 构建完成!"
echo ""

# 显示结果
ls -lh bray-core-windows-amd64.exe
echo ""
echo "=== 构建信息 ==="
echo "构建 commit:  ${COMMID}"
echo "输出文件:     bray-core-windows-amd64.exe"
echo ""

# 尝试压缩 (可选)
if command -v upx &> /dev/null; then
    echo "[5/5] UPX 压缩..."
    upx --best --lzma bray-core-windows-amd64.exe 2>/dev/null || echo "UPX 压缩失败，跳过"
    ls -lh bray-core-windows-amd64.exe
else
    echo "[5/5] UPX 未安装，跳过压缩"
    echo "  安装: scoop install upx  或  choco install upx"
fi
