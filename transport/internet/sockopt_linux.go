package internet

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/xtls/xray-core/common/errors"
	"golang.org/x/sys/unix"
)

// autoNotsentLowat is initialized on startup based on system memory.
// It provides a safe default for TCP_NOTSENT_LOWAT when the user hasn't
// configured an explicit value. Zero means "detection failed; skip."
var autoNotsentLowat int32

// sysDefaultCongestion stores the dynamically detected system default congestion control.
// Fallback path: Current sysctl -> BBR -> CUBIC.
var sysDefaultCongestion string = "bbr"

func init() {
	// Enable TCP_QUICKACK on accepted connections.
	// Disables delayed ACK for faster TLS handshake completion.
	setQuickAck = func(fd uintptr) {
		syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_QUICKACK, 1)
	}

	// -----------------------------------------------------------------
	// 动态检测拥塞控制算法优先级：当前 sysctl 默认值 > BBR > CUBIC
	// -----------------------------------------------------------------
	if data, err := os.ReadFile("/proc/sys/net/ipv4/tcp_congestion_control"); err == nil {
		sysCongest := strings.TrimSpace(string(data))
		if sysCongest != "" {
			sysDefaultCongestion = sysCongest
		}
	} else {
		// 若因特殊权限/沙盒无法读取当前默认值，则检测系统中注册的所有可用算法列表
		if availData, err := os.ReadFile("/proc/sys/net/ipv4/tcp_available_congestion_control"); err == nil {
			availStr := string(availData)
			availAlgorithms := strings.Fields(availStr) // 按空格切分成切片

			hasBBR := false
			hasCubic := false
			for _, algo := range availAlgorithms {
				if algo == "bbr" {
					hasBBR = true
				}
				if algo == "cubic" {
					hasCubic = true
				}
			}

			if hasBBR {
				sysDefaultCongestion = "bbr"
			} else if hasCubic {
				sysDefaultCongestion = "cubic"
			} else {
				sysDefaultCongestion = "cubic" // 最终兜底
			}
		}
	}
	// -----------------------------------------------------------------

	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		return
	}
	totalMB := uint64(info.Totalram) * uint64(info.Unit) / (1024 * 1024)
	switch {
	case totalMB < 512:
		autoNotsentLowat = 8192 // 8 KB  — tiny VPS, be conservative
	case totalMB < 2048:
		autoNotsentLowat = 16384 // 16 KB — typical 100 Mbps / 1–2 GB
	default:
		autoNotsentLowat = 32768 // 32 KB — high-bandwidth
	}
}

// resolveNotsentLowat returns the effective TCP_NOTSENT_LOWAT value and
// whether the caller explicitly set it. When the user supplied a value
// (> 0) we honour it; otherwise the auto-detected default is used.
func resolveNotsentLowat(userValue int32) (value int32, userExplicit bool) {
	if userValue > 0 {
		return userValue, true
	}
	return autoNotsentLowat, false
}

// applyOutboundSocketOptions applies socket options for outbound connection.
// note that unlike other part of Xray, this function needs network with speified network stack(tcp4/tcp6/udp4/udp6)
func applyOutboundSocketOptions(network string, address string, fd uintptr, config *SocketConfig) error {
	if config.Mark != 0 {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_MARK, int(config.Mark)); err != nil {
			return errors.New("failed to set SO_MARK").Base(err)
		}
	}

	if config.Interface != "" {
		if err := syscall.BindToDevice(int(fd), config.Interface); err != nil {
			return errors.New("failed to set Interface").Base(err)
		}
	}

	if isTCPSocket(network) {
		tfo := config.ParseTFOValue()
		if tfo > 0 {
			tfo = 1
		}
		if tfo >= 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.SOL_TCP, unix.TCP_FASTOPEN_CONNECT, tfo); err != nil {
				return errors.New("failed to set TCP_FASTOPEN_CONNECT", tfo).Base(err)
			}
		}

		// 优化：不再写死 "bbr"，无配置时自动回退到探测出的系统最优算法
		if config.TcpCongestion == "" {
			config.TcpCongestion = sysDefaultCongestion
		}
		if err := syscall.SetsockoptString(int(fd), syscall.SOL_TCP, syscall.TCP_CONGESTION, config.TcpCongestion); err != nil {
			return errors.New("failed to set TCP_CONGESTION", err)
		}

		if config.TcpWindowClamp > 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_WINDOW_CLAMP, int(config.TcpWindowClamp)); err != nil {
				return errors.New("failed to set TCP_WINDOW_CLAMP", err)
			}
		}

		if config.TcpUserTimeout > 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(config.TcpUserTimeout)); err != nil {
				return errors.New("failed to set TCP_USER_TIMEOUT", err)
			}
		}

		if config.TcpMaxSeg > 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_MAXSEG, int(config.TcpMaxSeg)); err != nil {
				return errors.New("failed to set TCP_MAXSEG", err)
			}
		}

		if lowat, explicit := resolveNotsentLowat(config.TcpNotsentLowat); lowat > 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_NOTSENT_LOWAT, int(lowat)); err != nil {
				if explicit {
					return errors.New("failed to set TCP_NOTSENT_LOWAT", err)
				}
				// auto-detected value on old kernel — silently skip
			}
		}

	}

	if len(config.CustomSockopt) > 0 {
		for _, custom := range config.CustomSockopt {
			if custom.System != "" && custom.System != runtime.GOOS {
				errors.LogDebug(context.Background(), "CustomSockopt system not match: ", "want ", custom.System, " got ", runtime.GOOS)
				continue
			}
			// Skip unwanted network type
			// network might be tcp4 or tcp6
			// use HasPrefix so that "tcp" can match tcp4/6 with "tcp" if user want to control all tcp (udp is also the same)
			// if it is empty, strings.HasPrefix will always return true to make it apply for all networks
			if !strings.HasPrefix(network, custom.Network) {
				continue
			}
			level := 0x6 // default TCP
			var opt int
			if len(custom.Opt) == 0 {
				return errors.New("No opt!")
			} else {
				opt, _ = strconv.Atoi(custom.Opt)
			}
			if custom.Level != "" {
				level, _ = strconv.Atoi(custom.Level)
			}
			if custom.Type == "int" {
				value, _ := strconv.Atoi(custom.Value)
				if err := syscall.SetsockoptInt(int(fd), level, opt, value); err != nil {
					return errors.New("failed to set CustomSockoptInt", opt, value, err)
				}
			} else if custom.Type == "str" {
				if err := syscall.SetsockoptString(int(fd), level, opt, custom.Value); err != nil {
					return errors.New("failed to set CustomSockoptString", opt, custom.Value, err)
				}
			} else {
				return errors.New("unknown CustomSockopt type:", custom.Type)
			}
		}
	}

	if config.Tproxy.IsEnabled() {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_IP, syscall.IP_TRANSPARENT, 1); err != nil {
			return errors.New("failed to set IP_TRANSPARENT").Base(err)
		}
	}

	return nil
}

// applyInboundSocketOptions applies socket options for inbound listener.
// note that unlike other part of Xray, this function needs network with speified network stack(tcp4/tcp6/udp4/udp6)
func applyInboundSocketOptions(network string, fd uintptr, config *SocketConfig) error {
	if config.Mark != 0 {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_MARK, int(config.Mark)); err != nil {
			return errors.New("failed to set SO_MARK").Base(err)
		}
	}
	if isTCPSocket(network) {
		tfo := config.ParseTFOValue()
		if tfo >= 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.SOL_TCP, unix.TCP_FASTOPEN, tfo); err != nil {
				return errors.New("failed to set TCP_FASTOPEN", tfo).Base(err)
			}
		}
		// TCP_DEFER_ACCEPT delays accept() until the first data byte
		// arrives, saving one wakeup per connection. For TLS servers
		// this means the ClientHello is already in the buffer when
		// accept() returns. 3s is safe — any client that can't send
		// the ClientHello in 3s is almost certainly not real traffic.
		if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_DEFER_ACCEPT, 3); err != nil {
			// Non-fatal: old kernels may not support this.
		}

		if config.TcpKeepAliveInterval > 0 || config.TcpKeepAliveIdle > 0 {
			if config.TcpKeepAliveInterval > 0 {
				if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, int(config.TcpKeepAliveInterval)); err != nil {
					return errors.New("failed to set TCP_KEEPINTVL", err)
				}
			}
			if config.TcpKeepAliveIdle > 0 {
				if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, int(config.TcpKeepAliveIdle)); err != nil {
					return errors.New("failed to set TCP_KEEPIDLE", err)
				}
			}
			if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1); err != nil {
				return errors.New("failed to set SO_KEEPALIVE", err)
			}
		} else if config.TcpKeepAliveInterval < 0 || config.TcpKeepAliveIdle < 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 0); err != nil {
				return errors.New("failed to unset SO_KEEPALIVE", err)
			}
		}

		// 优化：不再写死 "bbr"，无配置时自动回退到探测出的系统最优算法
		if config.TcpCongestion == "" {
			config.TcpCongestion = sysDefaultCongestion
		}
		if err := syscall.SetsockoptString(int(fd), syscall.SOL_TCP, syscall.TCP_CONGESTION, config.TcpCongestion); err != nil {
			return errors.New("failed to set TCP_CONGESTION", err)
		}

		if config.TcpWindowClamp > 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_WINDOW_CLAMP, int(config.TcpWindowClamp)); err != nil {
				return errors.New("failed to set TCP_WINDOW_CLAMP", err)
			}
		}

		if config.TcpUserTimeout > 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(config.TcpUserTimeout)); err != nil {
				return errors.New("failed to set TCP_USER_TIMEOUT", err)
			}
		}

		if config.TcpMaxSeg > 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_MAXSEG, int(config.TcpMaxSeg)); err != nil {
				return errors.New("failed to set TCP_MAXSEG", err)
			}
		}

		if lowat, explicit := resolveNotsentLowat(config.TcpNotsentLowat); lowat > 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_NOTSENT_LOWAT, int(lowat)); err != nil {
				if explicit {
					return errors.New("failed to set TCP_NOTSENT_LOWAT", err)
				}
				// auto-detected value on old kernel — silently skip
			}
		}
		if len(config.CustomSockopt) > 0 {
			for _, custom := range config.CustomSockopt {
				if custom.System != "" && custom.System != runtime.GOOS {
					errors.LogDebug(context.Background(), "CustomSockopt system not match: ", "want ", custom.System, " got ", runtime.GOOS)
					continue
				}
				// Skip unwanted network type
				// network might be tcp4 or tcp6
				// use HasPrefix so that "tcp" can match tcp4/6 with "tcp" if user want to control all tcp (udp is also the same)
				// if it is empty, strings.HasPrefix will always return true to make it apply for all networks
				if !strings.HasPrefix(network, custom.Network) {
					continue
				}
				level := 0x6 // default TCP
				var opt int
				if len(custom.Opt) == 0 {
					return errors.New("No opt!")
				} else {
					opt, _ = strconv.Atoi(custom.Opt)
				}
				if custom.Level != "" {
					level, _ = strconv.Atoi(custom.Level)
				}
				if custom.Type == "int" {
					value, _ := strconv.Atoi(custom.Value)
					if err := syscall.SetsockoptInt(int(fd), level, opt, value); err != nil {
						return errors.New("failed to set CustomSockoptInt", opt, value, err)
					}
				} else if custom.Type == "str" {
					if err := syscall.SetsockoptString(int(fd), level, opt, custom.Value); err != nil {
						return errors.New("failed to set CustomSockoptString", opt, custom.Value, err)
					}
				} else {
					return errors.New("unknown CustomSockopt type:", custom.Type)
				}
			}
		}
	}

	if config.Tproxy.IsEnabled() {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_IP, syscall.IP_TRANSPARENT, 1); err != nil {
			return errors.New("failed to set IP_TRANSPARENT").Base(err)
		}
	}

	if config.ReceiveOriginalDestAddress && isUDPSocket(network) {
		err1 := syscall.SetsockoptInt(int(fd), syscall.SOL_IPV6, unix.IPV6_RECVORIGDSTADDR, 1)
		err2 := syscall.SetsockoptInt(int(fd), syscall.SOL_IP, syscall.IP_RECVORIGDSTADDR, 1)
		if err1 != nil && err2 != nil {
			return err1
		}
	}

	if config.V6Only {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_IPV6, syscall.IPV6_V6ONLY, 1); err != nil {
			return errors.New("failed to set IPV6_V6ONLY", err)
		}
	}

	return nil
}

func setReuseAddr(fd uintptr) error {
	if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return errors.New("failed to set SO_REUSEADDR").Base(err).AtWarning()
	}
	return nil
}

func setReusePort(fd uintptr) error {
	if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		return errors.New("failed to set SO_REUSEPORT").Base(err).AtWarning()
	}
	return nil
}
