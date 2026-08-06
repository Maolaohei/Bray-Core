package main

// Report rendering: Markdown comparison report + JSON artifact.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type reportMeta struct {
	GeneratedAt string            `json:"generated_at"`
	Suites      []string          `json:"suites"`
	Targets     []string          `json:"targets"`
	Count       int               `json:"count"`
	Quick       bool              `json:"quick"`
	Commits     map[string]string `json:"commits"`
	Benchstat   bool              `json:"benchstat"`
}

type jsonReport struct {
	Meta   reportMeta    `json:"meta"`
	Runs   []*benchRun   `json:"runs"`
	Suites []*comparison `json:"suites"`
}

func writeReports(outDir string, meta reportMeta, runs []*benchRun, comps []*comparison, benchstatText map[string]string) (string, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", err
	}
	md := renderMarkdown(meta, runs, comps, benchstatText)
	mdPath := filepath.Join(outDir, "report.md")
	if err := os.WriteFile(mdPath, []byte(md), 0o644); err != nil {
		return "", err
	}
	jr := jsonReport{Meta: meta, Runs: runs, Suites: comps}
	jsonPath := filepath.Join(outDir, "report.json")
	if b, err := json.MarshalIndent(jr, "", "  "); err == nil {
		_ = os.WriteFile(jsonPath, b, 0o644)
	}
	return mdPath, nil
}

func renderMarkdown(meta reportMeta, runs []*benchRun, comps []*comparison, benchstatText map[string]string) string {
	var b strings.Builder
	b.WriteString("# Benchmark Comparison Report\n\n")
	b.WriteString(fmt.Sprintf("- Generated: `%s`\n", meta.GeneratedAt))
	b.WriteString(fmt.Sprintf("- Suites: `%s`\n", strings.Join(meta.Suites, ", ")))
	b.WriteString(fmt.Sprintf("- Targets: `%s`\n", strings.Join(meta.Targets, ", ")))
	b.WriteString(fmt.Sprintf("- count: `%d` · quick: `%v`\n", meta.Count, meta.Quick))
	if len(meta.Commits) > 0 {
		b.WriteString("- Commits:\n")
		for _, n := range sortedKeys(meta.Commits) {
			b.WriteString(fmt.Sprintf("  - `%s` @ `%s`\n", n, meta.Commits[n]))
		}
	}
	b.WriteString("\nDelta = (Bray − Upstream) / Upstream × 100%，负数表示 Bray 更快。|Δ| ≤ 3% 记为 tie。\n\n")

	for _, c := range comps {
		b.WriteString(fmt.Sprintf("\n## Suite `%s`\n\n", c.Suite))
		b.WriteString(fmt.Sprintf("**覆盖**: both %d · bray-only %d · upstream-only %d ｜ **结果**: Bray 更快 %d · tie %d · 上游更快 %d\n\n",
			c.Counts.Both, c.Counts.BrayOnly, c.Counts.UpOnly, c.Counts.BrayFaster, c.Counts.Ties, c.Counts.UpFaster))

		b.WriteString("| Benchmark | 覆盖 | Bray ns/op | Upstream ns/op | Δ% | 判定 | Bray MB/s | Up MB/s | Bray B/op | Up B/op | Bray allocs | Up allocs |\n")
		b.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|\n")
		for _, p := range c.Pairs {
			cover := "both"
			if p.Verdict == "bray-only" {
				cover = "Bray ✗上游"
			} else if p.Verdict == "upstream-only" {
				cover = "上游 ✗Bray"
			}
			ns := func(v float64, ok bool) string {
				if !ok {
					return "—"
				}
				return fmt.Sprintf("%.1f", v)
			}
			mbs := func(v float64, ok bool) string {
				if !ok || v == 0 {
					return "—"
				}
				return fmt.Sprintf("%.1f", v)
			}
			delta := "—"
			if p.Verdict == "bray-faster" || p.Verdict == "upstream-faster" || p.Verdict == "tie" {
				delta = fmt.Sprintf("%+.1f%%", p.DeltaPct)
			}
			verdict := p.Verdict
			switch p.Verdict {
			case "bray-faster":
				verdict = "🟢 Bray"
			case "upstream-faster":
				verdict = "🔴 上游"
			case "tie":
				verdict = "⚪ tie"
			case "bray-only":
				verdict = "Bray 独有"
			case "upstream-only":
				verdict = "上游独有"
			}
			mem := func(v float64, ok bool) string {
				if !ok || v == 0 {
					return "—"
				}
				return fmt.Sprintf("%.0f", v)
			}
			b.WriteString(fmt.Sprintf("| `%s` | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
				p.Name, cover,
				ns(p.BrayMed.NS, p.Bray != nil), ns(p.UpMed.NS, p.Up != nil),
				delta, verdict,
				mbs(p.BrayMed.MBPS, p.Bray != nil), mbs(p.UpMed.MBPS, p.Up != nil),
				mem(p.BrayB, p.HasMem), mem(p.UpB, p.HasMem),
				mem(p.BrayA, p.HasMem), mem(p.UpA, p.HasMem)))
		}

		if text, ok := benchstatText[c.Suite]; ok && strings.TrimSpace(text) != "" {
			b.WriteString("\n### benchstat（old=upstream, new=bray）\n\n```\n")
			b.WriteString(text)
			b.WriteString("```\n")
		}
	}
	return b.String()
}

func sortedKeys(m map[string]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func nowStamp() string {
	return time.Now().Format("2006-01-02T15:04:05")
}
