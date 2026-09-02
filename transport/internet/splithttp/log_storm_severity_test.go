package splithttp

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	clog "github.com/xtls/xray-core/common/log"
)

// TestStormDiagnosticsAreDebugLevel pins the severity of every log call in
// this package (2026-08-31 log-bloat report). During TLS/probe storms the
// 2026.08.31 binary emitted ~12 Info lines per failed request (184 lines /
// 15 requests, measured with a real vless+xhttp client against a self-signed
// TLS server). All per-request / per-attempt storm diagnostics were demoted
// to Debug; only the call sites whitelisted below remain at Info.
//
// Invariant enforced (ALLOWLIST, not denylist): every errors.LogInfo /
// errors.LogInfoInner call site in non-test package sources must carry a
// first string literal starting with one of the whitelisted one-shot /
// N-consecutive prefixes. Any new Info call on a per-request path fails this
// test — the author must use LogDebug/LogDebugInner, or consciously extend
// the whitelist with justification.
var allowedInfoMessagePrefixes = []string{
	// hub.go — one-shot / startup lines
	"Cloudflare CDN detected",
	"listening UNIX domain socket",
	"listening QUIC for XHTTP/3",
	"listening TCP for XHTTP",
	// mux.go — N-consecutive eviction counters (fire once per N failures)
	"XMUX: open-header timeout x",
	"XMUX: idle-beacon failure x",
	// mux.go — one-shot environment transitions
	"XMUX: network change confirmed",
	"XMUX: CDN edge detected via ",
}

// TestStormDiagnosticsAreDebugLevel parses every non-test .go file in this
// package and asserts each errors.LogInfo* call site's first string literal
// is whitelisted above.
func TestStormDiagnosticsAreDebugLevel(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var violations []string

	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, file, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok || ident.Name != "errors" {
				return true
			}
			if sel.Sel.Name != "LogInfo" && sel.Sel.Name != "LogInfoInner" {
				return true
			}

			pos := fset.Position(call.Pos())
			// Find the first string-literal argument (after ctx/inner).
			firstLit := ""
			for _, arg := range call.Args {
				if lit, ok := arg.(*ast.BasicLit); ok && lit.Kind == token.STRING {
					firstLit = strings.Trim(lit.Value, "\"`")
					break
				}
			}
			for _, prefix := range allowedInfoMessagePrefixes {
				if strings.HasPrefix(firstLit, prefix) {
					return true // whitelisted
				}
			}
			if firstLit == "" {
				violations = append(violations,
					pos.String()+": errors."+sel.Sel.Name+" with no string literal — storms hide in dynamic args; use LogDebug or add a literal")
				return true
			}
			violations = append(violations,
				pos.String()+": errors."+sel.Sel.Name+"(\""+firstLit+"...\") is not whitelisted — per-request/per-attempt diagnostics must use LogDebug/LogDebugInner (see 2026-08-31 log-bloat report)")
			return true
		})
	}

	if len(violations) > 0 {
		sort.Strings(violations)
		t.Errorf("non-whitelisted Info-level log call sites:\n%s",
			strings.Join(violations, "\n"))
	}
}

// TestLogSeverityOrdering pins the xray log.Severity enum ordering that
// app/log Instance.Handle's `msg.Severity <= ErrorLogLevel` filter depends
// on. Smaller value = more severe.
func TestLogSeverityOrdering(t *testing.T) {
	if !(clog.Severity_Unknown < clog.Severity_Error &&
		clog.Severity_Error < clog.Severity_Warning &&
		clog.Severity_Warning < clog.Severity_Info &&
		clog.Severity_Info < clog.Severity_Debug) {
		t.Fatal("xlog severity ordering broken: filter msg.Severity <= ErrorLogLevel depends on Unknown<Error<Warning<Info<Debug")
	}
}
