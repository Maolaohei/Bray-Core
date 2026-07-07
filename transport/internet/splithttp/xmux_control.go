package splithttp

// forceNewConnection when true disables XMUX connection pooling.
// Set via build tag: go build -tags=noxmux
var forceNewConnection bool
