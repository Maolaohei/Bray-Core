package encoding

import (
	"context"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/signal"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/proxy/vless"
	"io"
)

const (
	Version = byte(0)
)

var addrParser = protocol.NewAddressParser(
	protocol.AddressFamilyByte(byte(protocol.AddressTypeIPv4), net.AddressFamilyIPv4),
	protocol.AddressFamilyByte(byte(protocol.AddressTypeDomain), net.AddressFamilyDomain),
	protocol.AddressFamilyByte(byte(protocol.AddressTypeIPv6), net.AddressFamilyIPv6),
	protocol.PortThenAddress(),
)

// EncodeRequestHeader writes encoded request header into the given writer.
func EncodeRequestHeader(writer io.Writer, request *protocol.RequestHeader, requestAddons *Addons) error {
	buffer := buf.StackNew()
	defer buffer.Release()

	if err := buffer.WriteByte(request.Version); err != nil {
		return errors.New("failed to write request version").Base(err)
	}
	account, ok := request.User.Account.(*vless.MemoryAccount)
	if !ok {
		return errors.New("unsupported account type")
	}
	if _, err := buffer.Write(account.ID.Bytes()); err != nil {
		return errors.New("failed to write request user id").Base(err)
	}
	if err := EncodeHeaderAddons(&buffer, requestAddons); err != nil {
		return errors.New("failed to encode request header addons").Base(err)
	}

	if err := buffer.WriteByte(byte(request.Command)); err != nil {
		return errors.New("failed to write request command").Base(err)
	}

	if request.Command != protocol.RequestCommandMux && request.Command != protocol.RequestCommandRvs {
		if err := addrParser.WriteAddressPort(&buffer, request.Address, request.Port); err != nil {
			return errors.New("failed to write request address and port").Base(err)
		}
	}

	if _, err := writer.Write(buffer.Bytes()); err != nil {
		return errors.New("failed to write request header").Base(err)
	}

	return nil
}

// DecodeRequestHeader decodes and returns (if successful) a RequestHeader from an input stream.
func DecodeRequestHeader(ctx context.Context, isfb bool, first *buf.Buffer, reader io.Reader, validator vless.Validator) ([]byte, *protocol.RequestHeader, *Addons, bool, error) {
	buffer := buf.StackNew()
	defer buffer.Release()

	request := new(protocol.RequestHeader)

	var id [16]byte

	if isfb {
		request.Version = first.Byte(0)
		if request.Version != 0 {
			return nil, nil, nil, isfb, errors.New("invalid request version")
		}
		copy(id[:], first.BytesRange(1, 17))
		if request.User = validator.Get(id); request.User == nil {
			// Do not embed the full UUID in the error text (green-zone): probes and
			// logs should not echo candidate user ids.
			return nil, nil, nil, isfb, errors.New("invalid request user id")
		}
		first.Advance(17)
	} else {
		// Read version byte first. If invalid, fail fast without reading
		// the full 17-byte header — avoids blocking for ~4s on short/abusive
		// connections that send a version byte other than 0.
		if _, err := buffer.ReadFullFrom(reader, 1); err != nil {
			return nil, nil, nil, false, errors.New("failed to read request version").Base(err)
		}
		request.Version = buffer.Byte(0)
		if request.Version != 0 {
			return nil, nil, nil, false, errors.New("invalid request version")
		}
		// Read remaining 16 bytes (UUID) now that version is confirmed.
		buffer.Clear()
		if _, err := buffer.ReadFullFrom(reader, 16); err != nil {
			return nil, nil, nil, false, errors.New("failed to read request user id").Base(err)
		}
		copy(id[:], buffer.Bytes())
		if request.User = validator.Get(id); request.User == nil {
			return nil, nil, nil, false, errors.New("invalid request user id")
		}
	}

	requestAddons := GetAddons()
	if err := DecodeHeaderAddons(&buffer, reader, requestAddons); err != nil {
		PutAddons(requestAddons)
		return nil, nil, nil, false, errors.New("failed to decode request header addons").Base(err)
	}

	buffer.Clear()
	if _, err := buffer.ReadFullFrom(reader, 1); err != nil {
		return nil, nil, nil, false, errors.New("failed to read request command").Base(err)
	}

	request.Command = protocol.RequestCommand(buffer.Byte(0))
	switch request.Command {
	case protocol.RequestCommandMux:
		request.Address = net.DomainAddress("v1.mux.cool")
	case protocol.RequestCommandRvs:
		request.Address = net.DomainAddress("v1.rvs.cool")
	case protocol.RequestCommandTCP, protocol.RequestCommandUDP:
		if addr, port, err := addrParser.ReadAddressPort(&buffer, reader); err == nil {
			request.Address = addr
			request.Port = port
		}
	}
	if request.Address == nil {
		return nil, nil, nil, false, errors.New("invalid request address")
	}
	return id[:], request, requestAddons, false, nil
}

// EncodeResponseHeader writes encoded response header into the given writer.
func EncodeResponseHeader(writer io.Writer, request *protocol.RequestHeader, responseAddons *Addons) error {
	buffer := buf.StackNew()
	defer buffer.Release()

	if err := buffer.WriteByte(request.Version); err != nil {
		return errors.New("failed to write response version").Base(err)
	}

	if err := EncodeHeaderAddons(&buffer, responseAddons); err != nil {
		return errors.New("failed to encode response header addons").Base(err)
	}

	if _, err := writer.Write(buffer.Bytes()); err != nil {
		return errors.New("failed to write response header").Base(err)
	}

	return nil
}

// DecodeResponseHeader decodes and returns (if successful) a ResponseHeader from an input stream.
func DecodeResponseHeader(reader io.Reader, request *protocol.RequestHeader) (*Addons, error) {
	// Hot path: the response header is a 1-byte version + a tiny addons blob
	// (length is a single byte, so at most 255 bytes). Avoid a pooled 8KB
	// buf.Buffer here — buf.StackNew costs ~2 allocs and escapes on this path —
	// and decode straight from stack buffers instead.
	var hdr [2]byte
	if _, err := io.ReadFull(reader, hdr[:1]); err != nil {
		return nil, errors.New("failed to read response version").Base(err)
	}

	if hdr[0] != request.Version {
		return nil, errors.New("unexpected response version. Expecting ", int(request.Version), " but actually ", int(hdr[0]))
	}

	responseAddons := GetAddons()
	if _, err := io.ReadFull(reader, hdr[1:2]); err != nil {
		PutAddons(responseAddons)
		return nil, errors.New("failed to decode response header addons").Base(errAddonsProtobufLength)
	}

	if length := int(hdr[1]); length != 0 {
		var data [64]byte
		buf := data[:length]
		if length > len(data) {
			buf = make([]byte, length)
		}
		if _, err := io.ReadFull(reader, buf); err != nil {
			PutAddons(responseAddons)
			return nil, errors.New("failed to decode response header addons").Base(errAddonsProtobufValue)
		}
		if err := unmarshalAddons(buf, responseAddons); err != nil {
			PutAddons(responseAddons)
			return nil, errors.New("failed to decode response header addons").Base(err)
		}
	}

	return responseAddons, nil
}

// XtlsRead can switch to splice copy
func XtlsRead(reader buf.Reader, writer buf.Writer, timer *signal.ActivityTimer, conn net.Conn, trafficState *proxy.TrafficState, isUplink bool, ctx context.Context) error {
	err := func() error {
		for {
			if isUplink && trafficState.Inbound.UplinkReaderDirectCopy || !isUplink && trafficState.Outbound.DownlinkReaderDirectCopy {
				var writerConn net.Conn
				var inTimer *signal.ActivityTimer
				if inbound := session.InboundFromContext(ctx); inbound != nil && inbound.Conn != nil {
					writerConn = inbound.Conn
					inTimer = inbound.Timer
				}
				return proxy.CopyRawConnIfExist(ctx, conn, writerConn, writer, timer, inTimer)
			}
			buffer, err := reader.ReadMultiBuffer()
			if !buffer.IsEmpty() {
				timer.Update()
				if werr := writer.WriteMultiBuffer(buffer); werr != nil {
					return werr
				}
			}
			if err != nil {
				return err
			}
		}
	}()
	if err != nil && errors.Cause(err) != io.EOF {
		return err
	}
	return nil
}
