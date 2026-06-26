package mux_test

import (
	"io"
	"testing"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	. "github.com/xtls/xray-core/common/mux"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport/pipe"
)

func readAll(reader buf.Reader) (buf.MultiBuffer, error) {
	var mb buf.MultiBuffer
	for {
		b, err := reader.ReadMultiBuffer()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		mb = append(mb, b...)
	}
	return mb, nil
}

func TestReaderWriter_SingleSession(t *testing.T) {
	pReader, pWriter := pipe.New(pipe.WithSizeLimit(1024))

	dest := net.TCPDestination(net.DomainAddress("example.com"), 80)
	writer := NewWriter(1, dest, pWriter, protocol.TransferTypeStream, [8]byte{}, &session.Inbound{})

	writePayload := func(w *Writer, payload ...byte) error {
		b := buf.New()
		b.Write(payload)
		return w.WriteMultiBuffer(buf.MultiBuffer{b})
	}

	common.Must(writePayload(writer, 'a', 'b', 'c', 'd'))
	writer.Close()

	pWriter.Close()

	bytesReader := &buf.BufferedReader{Reader: pReader}

	var meta FrameMetadata
	common.Must(meta.Unmarshal(bytesReader, false))
	if meta.SessionID != 1 {
		t.Errorf("session id = %d, want 1", meta.SessionID)
	}
	if meta.SessionStatus != SessionStatusNew {
		t.Errorf("status = %d, want New", meta.SessionStatus)
	}
	if meta.Target != dest {
		t.Errorf("target = %v, want %v", meta.Target, dest)
	}
	if meta.Option&OptionData == 0 {
		t.Error("OptionData not set")
	}

	data, err := readAll(NewStreamReader(bytesReader))
	common.Must(err)
	if s := data.String(); s != "abcd" {
		t.Errorf("data = %q, want %q", s, "abcd")
	}

	var endMeta FrameMetadata
	common.Must(endMeta.Unmarshal(bytesReader, false))
	if endMeta.SessionStatus != SessionStatusEnd {
		t.Errorf("end status = %d, want End", endMeta.SessionStatus)
	}
}

func TestReaderWriter_MultiSession(t *testing.T) {
	pReader, pWriter := pipe.New(pipe.WithSizeLimit(4096))

	dest1 := net.TCPDestination(net.DomainAddress("example.com"), 80)
	dest2 := net.TCPDestination(net.LocalHostIP, 443)

	w1 := NewWriter(1, dest1, pWriter, protocol.TransferTypeStream, [8]byte{}, &session.Inbound{})
	w2 := NewWriter(2, dest2, pWriter, protocol.TransferTypeStream, [8]byte{}, &session.Inbound{})

	writePayload := func(w *Writer, payload ...byte) error {
		b := buf.New()
		b.Write(payload)
		return w.WriteMultiBuffer(buf.MultiBuffer{b})
	}

	common.Must(writePayload(w1, 'a', 'b'))
	common.Must(writePayload(w2, 'x', 'y'))
	common.Must(writePayload(w1, 'c', 'd'))
	w1.Close()
	common.Must(writePayload(w2, 'z'))
	w2.Close()

	pWriter.Close()

	rawMB, _ := pReader.ReadMultiBuffer()
	var raw []byte
	for _, b := range rawMB {
		if b != nil {
			raw = append(raw, b.BytesTo(b.Len())...)
		}
	}
	t.Logf("RAW (%d bytes): %x", len(raw), raw)

	bytesReader := &buf.BufferedReader{Buffer: rawMB}

	type parsedFrame struct {
		meta   FrameMetadata
		data   string
		hasData bool
	}

	var frames []parsedFrame
	for {
		var meta FrameMetadata
		err := meta.Unmarshal(bytesReader, false)
		if err != nil {
			if err == io.EOF || err == io.ErrClosedPipe {
				break
			}
			t.Fatalf("unmarshal error: %v", err)
		}
		f := parsedFrame{meta: meta}
		if meta.Option&OptionData != 0 {
			dl, err := serial.ReadUint16(bytesReader)
			if err != nil {
				t.Fatalf("read data len: %v", err)
			}
			dataBuf := buf.New()
			dataBuf.ReadFullFrom(bytesReader, int32(dl))
			f.data = string(dataBuf.BytesTo(int32(dl)))
			f.hasData = true
		}
		frames = append(frames, f)
	}

	type sessionResult struct {
		newTarget net.Destination
		newFound  bool
		endFound  bool
		dataAll   string
	}

	results := map[uint16]*sessionResult{}
	for _, f := range frames {
		id := f.meta.SessionID
		if results[id] == nil {
			results[id] = &sessionResult{}
		}
		r := results[id]
		switch f.meta.SessionStatus {
		case SessionStatusNew:
			r.newFound = true
			r.newTarget = f.meta.Target
			if f.hasData {
				r.dataAll += f.data
			}
		case SessionStatusKeep:
			if f.hasData {
				r.dataAll += f.data
			}
		case SessionStatusEnd:
			r.endFound = true
		}
	}

	if len(frames) != 6 {
		t.Errorf("total frames = %d, want 6", len(frames))
	}

	r1 := results[1]
	if r1 == nil || !r1.newFound {
		t.Error("session 1: New frame missing")
	} else if r1.newTarget != dest1 {
		t.Errorf("session 1: target = %v, want %v", r1.newTarget, dest1)
	}
	if r1 != nil && r1.dataAll != "abcd" {
		t.Errorf("session 1: data = %q, want %q", r1.dataAll, "abcd")
	}
	if r1 != nil && !r1.endFound {
		t.Error("session 1: End frame missing")
	}

	r2 := results[2]
	if r2 == nil || !r2.newFound {
		t.Error("session 2: New frame missing")
	} else if r2.newTarget != dest2 {
		t.Errorf("session 2: target = %v, want %v", r2.newTarget, dest2)
	}
	if r2 != nil && r2.dataAll != "xyz" {
		t.Errorf("session 2: data = %q, want %q", r2.dataAll, "xyz")
	}
	if r2 != nil && !r2.endFound {
		t.Error("session 2: End frame missing")
	}
}
