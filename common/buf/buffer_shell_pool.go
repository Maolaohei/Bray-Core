package buf

import "sync"

// bufferShellPool reuses *Buffer objects for FromBytes wrappers so hot paths
// that wrap external durable slices (packet-up retries) do not allocate a
// Buffer shell on every call. Only unmanaged shells are returned here.
var bufferShellPool = sync.Pool{
	New: func() any {
		return &Buffer{}
	},
}
