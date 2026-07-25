#!/usr/bin/env python3
"""Format Bray-Core benchmark results into Markdown tables + SVG + history.

Reads go test -bench output (or sanitized benchstat-compatible lines) from
bench_results/{base,new}_*.txt and writes:
  - bench_results/report.md          (human report for CI comments)
  - bench_results/summary.json       (machine summary for this run)
  - bench_results/summary.svg        (simple bar chart of key deltas)
  - bench_results/history/<id>.json  (optional archive when --history)
  - bench_results/history/latest.md  (latest summary snapshot)

GitHub Markdown cannot reliably color <span>; we use emoji trends instead:
  🟢 improved (lower latency / higher throughput)
  🔴 regression
  ⚪ stable / noise
  🆕 new metric (no baseline)
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

BENCH_RE = re.compile(
    r"^Benchmark(?P<name>[A-Za-z0-9_./-]+?)(?:-(?P<procs>\d+))?"
    r"\s+(?P<iters>\d+)"
    r"(?:\s+(?P<ns>[\d.]+)\s+ns/op)?"
    r"(?:\s+(?P<mb>[\d.]+)\s+MB/s)?"
    r"(?:\s+(?P<bop>[\d.]+)\s+B/op)?"
    r"(?:\s+(?P<aop>[\d.]+)\s+allocs/op)?"
)

STABLE_PCT = 3.0  # |delta| < 3% counts as stable

# Suites that represent Bray product advantages (not common/buf regression noise).
ADVANTAGE_SUITES = frozenset({"xhttp_core", "xmux"})
# Prefer these metric name fragments when ranking Advantage Highlights.
ADVANTAGE_NAME_HINTS = (
    "TTFB",
    "Burst",
    "MemoryAllocations",
    "Modes",
    "GetRequestHeader",
    "FormatSeq",
    "GetXmuxClient",
    "PoolScheduling",
    "ConcurrentReadWrite",
    "RTTEWMA",
)



def load_upstream_snapshot(path: Path) -> dict:
    """Load fixed external Xray-core reference metrics.

    JSON shape:
      { "label": str, "metrics": { Name: { unit, value, higher_is_better? } } }
    Names are bare benchmark tails (no Benchmark prefix), matched by endswith.
    """
    if not path or not Path(path).is_file():
        return {}
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    metrics = data.get("metrics") or {}
    out = {"label": data.get("label") or Path(path).name, "metrics": {}}
    for k, v in metrics.items():
        if not isinstance(v, dict) or "value" not in v:
            continue
        unit = v.get("unit") or "ns/op"
        hib = v.get("higher_is_better")
        if hib is None:
            hib = str(unit).upper().endswith("MB/S") or str(unit).endswith("/s")
        out["metrics"][k] = {
            "unit": unit,
            "value": float(v["value"]),
            "higher_is_better": bool(hib),
        }
    return out


def match_upstream(name: str, upstream_metrics: dict):
    """Match go bench name to upstream key (exact or suffix)."""
    if not upstream_metrics:
        return None
    if name in upstream_metrics:
        return name, upstream_metrics[name]
    for k, v in upstream_metrics.items():
        if name == k or name.endswith(k) or name.endswith("/" + k):
            return k, v
        if k.endswith(name) and len(name) >= 4:
            return k, v
    return None



def parse_bench_file(path: Path) -> Dict[str, dict]:
    """Return name -> aggregated metrics (mean of all counts)."""
    raw: Dict[str, List[dict]] = defaultdict(list)
    if not path.is_file():
        return {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        m = BENCH_RE.match(line.strip())
        if not m:
            continue
        name = m.group("name")
        row = {}
        if m.group("ns") is not None:
            row["ns_op"] = float(m.group("ns"))
        if m.group("mb") is not None:
            row["mb_s"] = float(m.group("mb"))
        if m.group("bop") is not None:
            row["b_op"] = float(m.group("bop"))
        if m.group("aop") is not None:
            row["allocs_op"] = float(m.group("aop"))
        if row:
            raw[name].append(row)

    out: Dict[str, dict] = {}
    for name, rows in raw.items():
        agg: dict = {"samples": len(rows)}
        for key in ("ns_op", "mb_s", "b_op", "allocs_op"):
            vals = [r[key] for r in rows if key in r]
            if vals:
                agg[key] = statistics.fmean(vals)
        out[name] = agg
    return out


def primary_metric(row: dict) -> Tuple[str, float, bool]:
    """Return (label, value, higher_is_better). Prefer throughput when present."""
    if "mb_s" in row:
        return "MB/s", row["mb_s"], True
    if "ns_op" in row:
        return "ns/op", row["ns_op"], False
    if "b_op" in row:
        return "B/op", row["b_op"], False
    if "allocs_op" in row:
        return "allocs/op", row["allocs_op"], False
    return "?", 0.0, False


def fmt_num(v: float) -> str:
    if v is None:
        return "—"
    av = abs(v)
    if av >= 1_000_000:
        return f"{v:,.0f}"
    if av >= 10_000:
        return f"{v:,.1f}"
    if av >= 100:
        return f"{v:.2f}"
    if av >= 1:
        return f"{v:.3f}"
    return f"{v:.4f}"


def delta_pct(base: float, cur: float, higher_is_better: bool) -> Optional[float]:
    if base is None or cur is None or base == 0:
        return None
    # Positive delta = better when higher_is_better for throughput,
    # or lower latency for ns/op.
    if higher_is_better:
        return (cur - base) / base * 100.0
    return (base - cur) / base * 100.0


def trend_emoji(pct: Optional[float], has_base: bool) -> str:
    if not has_base or pct is None:
        return "🆕"
    if abs(pct) < STABLE_PCT:
        return "⚪"
    if pct > 0:
        return "🟢"
    return "🔴"


def trend_label(pct: Optional[float], has_base: bool) -> str:
    em = trend_emoji(pct, has_base)
    if not has_base or pct is None:
        return f"{em} new"
    sign = "+" if pct > 0 else ""
    if abs(pct) < STABLE_PCT:
        return f"{em} stable ({sign}{pct:.1f}%)"
    if pct > 0:
        return f"{em} improved ({sign}{pct:.1f}%)"
    return f"{em} slower ({sign}{pct:.1f}%)"


def load_suite(results_dir: Path, suite: str) -> Tuple[Dict[str, dict], Dict[str, dict]]:
    base = parse_bench_file(results_dir / f"base_{suite}.txt")
    # Prefer clean files if present
    if (results_dir / f"base_{suite}.clean.txt").is_file():
        base = parse_bench_file(results_dir / f"base_{suite}.clean.txt") or base
    new = parse_bench_file(results_dir / f"new_{suite}.txt")
    if (results_dir / f"new_{suite}.clean.txt").is_file():
        new = parse_bench_file(results_dir / f"new_{suite}.clean.txt") or new
    return base, new


def suite_table(
    suite: str,
    base: Dict[str, dict],
    new: Dict[str, dict],
    upstream: Optional[dict] = None,
) -> Tuple[str, List[dict]]:
    names = sorted(set(base) | set(new))
    if not names:
        return f"### {suite}\n\n_no benchmark lines_\n", []

    up_metrics = (upstream or {}).get("metrics") or {}
    has_any_up = any(match_upstream(n, up_metrics) for n in names)

    if suite == "xhttp_core":
        suite_title = f"{suite} — Bray XHTTP core (scenario-bound; not cross-comparable across modes)"
    else:
        suite_title = suite

    if has_any_up:
        lines = [
            f"### {suite_title}",
            "",
            "| Benchmark | Upstream | Self-baseline | Current | Δ vs Upstream | Δ vs Self | Trend |",
            "|-----------|---------:|--------------:|--------:|--------------:|----------:|-------|",
        ]
    else:
        lines = [
            f"### {suite_title}",
            "",
            "| Benchmark | Self-baseline | Current | Delta | Trend |",
            "|-----------|--------------:|--------:|------:|-------|",
        ]

    rows_out = []
    for name in names:
        b = base.get(name)
        n = new.get(name)
        if n is None:
            continue
        unit, cur_v, hib = primary_metric(n)
        if b:
            _, base_v, _ = primary_metric(b)
            if "mb_s" in b and "mb_s" in n:
                unit, base_v, cur_v, hib = "MB/s", b["mb_s"], n["mb_s"], True
            elif "ns_op" in b and "ns_op" in n:
                unit, base_v, cur_v, hib = "ns/op", b["ns_op"], n["ns_op"], False
            pct_self = delta_pct(base_v, cur_v, hib)
            base_s = f"{fmt_num(base_v)} {unit}"
            cur_s = f"{fmt_num(cur_v)} {unit}"
            if pct_self is None:
                delta_self_s = "—"
            else:
                sign = "+" if pct_self > 0 else ""
                delta_self_s = f"{sign}{pct_self:.1f}%"
            trend = trend_label(pct_self, True)
        else:
            base_s = "—"
            cur_s = f"{fmt_num(cur_v)} {unit}"
            delta_self_s = "—"
            pct_self = None
            trend = trend_label(None, False)
            base_v = None

        up_match = match_upstream(name, up_metrics)
        if up_match:
            _up_key, up = up_match
            up_unit = up["unit"]
            up_v = up["value"]
            up_hib = up["higher_is_better"]
            if up_unit == "MB/s" and "mb_s" in n:
                pct_up = delta_pct(up_v, n["mb_s"], True)
            elif up_unit == "ns/op" and "ns_op" in n:
                pct_up = delta_pct(up_v, n["ns_op"], False)
            else:
                pct_up = delta_pct(up_v, cur_v, up_hib)
            up_s = f"{fmt_num(up_v)} {up_unit}"
            if pct_up is None:
                delta_up_s = "—"
            else:
                sign = "+" if pct_up > 0 else ""
                delta_up_s = f"{sign}{pct_up:.1f}%"
        else:
            up_s = "—"
            delta_up_s = "—"
            pct_up = None
            up_v = None

        if has_any_up:
            lines.append(
                f"| `{name}` | {up_s} | {base_s} | {cur_s} | {delta_up_s} | {delta_self_s} | {trend} |"
            )
        else:
            lines.append(f"| `{name}` | {base_s} | {cur_s} | {delta_self_s} | {trend} |")

        rows_out.append(
            {
                "suite": suite,
                "name": name,
                "unit": unit,
                "upstream": up_v,
                "baseline": base_v if b else None,
                "current": cur_v,
                "delta_vs_upstream_pct": pct_up,
                "delta_pct": pct_self,
                "higher_is_better": hib,
                "trend": trend,
            }
        )
    lines.append("")
    return "\n".join(lines), rows_out



def advantage_score(row: dict) -> float:
    """Higher = more interesting for product Advantage Highlights."""
    name = row.get("name") or ""
    suite = row.get("suite") or ""
    score = 0.0
    if suite in ADVANTAGE_SUITES:
        score += 100.0
    for hint in ADVANTAGE_NAME_HINTS:
        if hint in name:
            score += 25.0
            break
    pct = row.get("delta_pct")
    if pct is not None:
        score += abs(float(pct))
        if pct >= STABLE_PCT:
            score += 10.0
    else:
        # first observation on an advantage suite still worth showing
        if suite in ADVANTAGE_SUITES:
            score += 8.0
    return score


def advantage_section(all_rows: List[dict], max_rows: int = 12) -> str:
    """Marketing/product board: Bray data-plane highlights (separate from regression counts)."""
    if not all_rows:
        return (
            "## Advantage Highlights\n\n"
            "_No metrics this run. Advantage suite is `xhttp_core` (+ XMUX hot paths)._\n"
        )

    preferred = [r for r in all_rows if (r.get("suite") in ADVANTAGE_SUITES)]
    pool = preferred if preferred else list(all_rows)
    ranked = sorted(pool, key=advantage_score, reverse=True)[:max_rows]

    lines = [
        "## Advantage Highlights",
        "",
        "> **Regression board ≠ Advantage board.**",
        "> Summary counts above answer: *did we get slower?*",
        "> This section answers: *where does Bray's XHTTP/XMUX data-plane show up?*",
        "> Prefer `xhttp_core` (TTFB / Burst / Modes / allocs / header+seq) and XMUX pool paths.",
        "> Do **not** cross-compare H2 vs H2C vs packet-up as one delta series.",
        "> Bray-only paths (session MAC, packet-up window semantics) may show Upstream as —.",
        "",
        "| Suite | Benchmark | Current | Self-baseline | Δ vs Self | Trend | Why it matters |",
        "|-------|-----------|--------:|--------------:|----------:|-------|----------------|",
    ]

    why_map = {
        "TTFB": "open latency / first byte",
        "Burst": "short transfer burst",
        "MemoryAllocations": "upload hot-path allocs",
        "Modes": "packet-up / stream-up / stream-one",
        "GetRequestHeader": "shared header construction",
        "FormatSeq": "zero-alloc sequence format",
        "GetXmuxClient": "mux client checkout",
        "PoolScheduling": "pool pick cost",
        "ConcurrentReadWrite": "mux concurrent RW",
        "RTTEWMA": "RTT EWMA update",
    }

    for r in ranked:
        name = r.get("name") or ""
        why = "data-plane metric"
        for k, v in why_map.items():
            if k in name:
                why = v
                break
        if r.get("suite") == "xhttp_core" and why == "data-plane metric":
            why = "XHTTP core (scenario-bound)"
        cur = f"{fmt_num(r['current'])} {r['unit']}"
        base = "—" if r.get("baseline") is None else f"{fmt_num(r['baseline'])} {r['unit']}"
        if r.get("delta_pct") is None:
            delta = "—"
        else:
            delta = f"{r['delta_pct']:+.1f}%"
        lines.append(
            f"| `{r['suite']}` | `{name}` | {cur} | {base} | {delta} | {r.get('trend', '—')} | {why} |"
        )

    adv_n = sum(1 for r in all_rows if r.get("suite") in ADVANTAGE_SUITES)
    lines.extend(
        [
            "",
            f"_Advantage-suite rows this run: **{adv_n}**. Full suite tables follow._",
            "",
        ]
    )
    return "\n".join(lines)


def summary_table(all_rows: List[dict]) -> str:
    improved = [r for r in all_rows if r.get("delta_pct") is not None and r["delta_pct"] >= STABLE_PCT]
    slower = [r for r in all_rows if r.get("delta_pct") is not None and r["delta_pct"] <= -STABLE_PCT]
    stable = [r for r in all_rows if r.get("delta_pct") is not None and abs(r["delta_pct"]) < STABLE_PCT]
    new = [r for r in all_rows if r.get("delta_pct") is None]

    lines = [
        "## Summary",
        "",
        "| Category | Count | Note |",
        "|----------|------:|------|",
        f"| 🟢 Improved (≥{STABLE_PCT:.0f}%) | {len(improved)} | lower latency / higher throughput |",
        f"| ⚪ Stable (±{STABLE_PCT:.0f}%) | {len(stable)} | noise band |",
        f"| 🔴 Slower (≤-{STABLE_PCT:.0f}%) | {len(slower)} | investigate if hot path |",
        f"| 🆕 New / no baseline | {len(new)} | first observation |",
        "",
    ]
    if not slower:
        lines.append("**Verdict: no material regression** vs self-baseline. See **Δ vs Upstream** column for fixed Xray-core reference.")
    else:
        lines.append("**Verdict: some regressions detected** — see 🔴 rows below.")
        lines.append("")
        lines.append("| Benchmark | Delta |")
        lines.append("|-----------|------:|")
        for r in sorted(slower, key=lambda x: x["delta_pct"])[:15]:
            lines.append(f"| `{r['suite']}/{r['name']}` | {r['delta_pct']:+.1f}% |")
    lines.append("")
    return "\n".join(lines)


def make_svg(all_rows: List[dict], path: Path, max_bars: int = 12) -> None:
    """Simple horizontal bar chart of |delta| for metrics with baseline."""
    scored = [r for r in all_rows if r.get("delta_pct") is not None]
    # Prefer Advantage-suite rows for the chart so common/buf noise does not dominate.
    adv_scored = [r for r in scored if r.get("suite") in ADVANTAGE_SUITES]
    if adv_scored:
        scored = adv_scored
    if not scored:
        path.write_text(
            '<svg xmlns="http://www.w3.org/2000/svg" width="640" height="80">'
            '<text x="16" y="40" font-family="sans-serif" font-size="14">'
            "No baseline deltas to chart</text></svg>\n",
            encoding="utf-8",
        )
        return

    scored.sort(key=lambda r: abs(r["delta_pct"]), reverse=True)
    scored = scored[:max_bars]
    width, left, right = 720, 220, 40
    row_h, top = 28, 36
    height = top + row_h * len(scored) + 24
    max_abs = max(abs(r["delta_pct"]) for r in scored) or 1.0
    bar_max = width - left - right - 60

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect width="100%" height="100%" fill="#0f1419"/>',
        f'<text x="16" y="22" fill="#e7ecf3" font-family="Segoe UI,sans-serif" font-size="14" font-weight="600">'
        f"Bray-Core advantage/self delta (top {len(scored)} by |%|)</text>",
    ]
    for i, r in enumerate(scored):
        y = top + i * row_h
        label = f"{r['suite']}/{r['name']}"
        if len(label) > 34:
            label = "…" + label[-33:]
        pct = r["delta_pct"]
        w = max(2, int(bar_max * abs(pct) / max_abs))
        color = "#3dd68c" if pct >= STABLE_PCT else ("#f07178" if pct <= -STABLE_PCT else "#8b949e")
        parts.append(
            f'<text x="12" y="{y + 16}" fill="#9aa4b2" font-family="Consolas,monospace" font-size="11">{label}</text>'
        )
        parts.append(
            f'<rect x="{left}" y="{y + 4}" width="{w}" height="16" rx="3" fill="{color}"/>'
        )
        parts.append(
            f'<text x="{left + w + 8}" y="{y + 16}" fill="#e7ecf3" font-family="Segoe UI,sans-serif" font-size="11">'
            f"{pct:+.1f}%</text>"
        )
    parts.append("</svg>\n")
    path.write_text("\n".join(parts), encoding="utf-8")


def build_report(
    results_dir: Path,
    suites: List[str],
    title: str,
    meta: dict,
) -> Tuple[str, List[dict]]:
    all_rows: List[dict] = []
    sections = [
        f"# {title}",
        "",
        f"- **Generated**: {meta.get('generated')}",
        f"- **Commit**: `{meta.get('sha', 'local')}`",
        f"- **Runner**: {meta.get('runner', 'unknown')}",
        f"- **Go**: {meta.get('go', 'unknown')}",
        f"- **Noise band**: ±{STABLE_PCT:.0f}% → ⚪ stable",
        "",
        "> Positive **Delta** means better (lower `ns/op` or higher `MB/s`).",
        "> Throughput rows that mix H2 / H2C / packet-up configs are **not** cross-comparable; only same-benchmark names are compared.",
        "",
    ]

    for suite in suites:
        base, new = load_suite(results_dir, suite)
        if not base and not new:
            continue
        md, rows = suite_table(suite, base, new)
        sections.append(md)
        all_rows.extend(rows)

    sections.insert(9, summary_table(all_rows))  # after meta blurb
    # actually summary after meta is better at top — rebuild carefully
    head = sections[:9]
    body = sections[9:]
    # body currently starts with suite sections only if we insert after — fix:
    # sections structure was meta then suites, then we inserted summary at 9 which is after meta blanks
    # Simpler: recompose
    return None, all_rows  # placeholder — real assembly below


def assemble_report(
    results_dir: Path,
    suites: List[str],
    meta: dict,
    upstream_path: Optional[str] = None,
) -> Tuple[str, List[dict], dict]:
    all_rows: List[dict] = []
    suite_mds: List[str] = []
    upstream = load_upstream_snapshot(Path(upstream_path)) if upstream_path else {}
    if not upstream:
        default_up = results_dir / "upstream" / "xray-core-v26.6.22.json"
        upstream = load_upstream_snapshot(default_up)
    for suite in suites:
        base, new = load_suite(results_dir, suite)
        if not base and not new:
            continue
        md, rows = suite_table(suite, base, new, upstream=upstream)
        suite_mds.append(md)
        all_rows.extend(rows)

    verdict_ok = not any(
        r.get("delta_pct") is not None and r["delta_pct"] <= -STABLE_PCT for r in all_rows
    )
    summary = {
        "generated": meta.get("generated"),
        "sha": meta.get("sha"),
        "runner": meta.get("runner"),
        "go": meta.get("go"),
        "upstream_label": (upstream or {}).get("label"),
        "stable_pct": STABLE_PCT,
        "counts": {
            "improved": sum(1 for r in all_rows if r.get("delta_pct") is not None and r["delta_pct"] >= STABLE_PCT),
            "stable": sum(1 for r in all_rows if r.get("delta_pct") is not None and abs(r["delta_pct"]) < STABLE_PCT),
            "slower": sum(1 for r in all_rows if r.get("delta_pct") is not None and r["delta_pct"] <= -STABLE_PCT),
            "new": sum(1 for r in all_rows if r.get("delta_pct") is None),
            "advantage_rows": sum(1 for r in all_rows if r.get("suite") in ADVANTAGE_SUITES),
        },
        "verdict": "no_material_regression" if verdict_ok else "regressions_present",
        "rows": all_rows,
    }

    parts = [
        f"# Bray-Core Benchmark Report",
        "",
        f"| Field | Value |",
        f"|-------|-------|",
        f"| Generated | {meta.get('generated')} |",
        f"| Commit | `{meta.get('sha', 'local')}` |",
        f"| Runner | {meta.get('runner', 'unknown')} |",
        f"| Go | {meta.get('go', 'unknown')} |",
        f"| Noise band | ±{STABLE_PCT:.0f}% → ⚪ |",
        f"| Upstream ref | {(upstream or {}).get('label') or '—'} |",
        f"| Tracks | Regression (all suites) · Advantage (`xhttp_core` + XMUX) |",
        "",
        "> **Delta 为正 = 更好**（`ns/op` 更低，或 `MB/s` 更高）。",
        "> **Upstream** = 固定外部 Xray-core 对照快照（common micro 参考，不是 Bray 卖点主表）。",
        "> **Self-baseline** = CI 上次 main 缓存。",
        "> **Regression board** = Summary counts（有没有明显变慢）。",
        "> **Advantage board** = XHTTP/XMUX 数据面（TTFB / Burst / Modes / allocs / pool）。",
        "> 仅同名 Benchmark 对比；H2 / H2C / packet-up 不同场景不要横向比较吞吐。",
        "",
        summary_table(all_rows),
        advantage_section(all_rows),
        "## Suites",
        "",
        *suite_mds,
        "## Chart",
        "",
        "![bench delta](summary.svg)",
        "",
        "_Chart prefers Advantage-suite deltas when present; otherwise largest |self-delta|._",
        "",
        "## How to read",
        "",
        "| Trend | Meaning |",
        "|-------|---------|",
        f"| 🟢 | improved ≥{STABLE_PCT:.0f}% vs **self-baseline** |",
        f"| ⚪ | within ±{STABLE_PCT:.0f}% (noise) |",
        f"| 🔴 | slower ≥{STABLE_PCT:.0f}% vs self-baseline |",
        "| 🆕 | no self-baseline for this metric |",
        "",
        "### Upstream vs Self-baseline",
        "",
        "| Column | Source |",
        "|--------|--------|",
        "| Upstream | Fixed external Xray-core snapshot (historical same-machine run; mostly common/*) |",
        "| Self-baseline | Last successful `main` CI cache (`base_*.txt`) |",
        "| Current | This commit |",
        "| Advantage Highlights | Product-facing subset (`xhttp_core`, XMUX); not a substitute for full suites |",
        "",
    ]
    return "\n".join(parts), all_rows, summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", default="bench_results")
    ap.add_argument("--suites", default="xhttp_core,xmux,happy,warmup,vless,buf")
    ap.add_argument("--sha", default="local")
    ap.add_argument("--runner", default="local")
    ap.add_argument("--go", default="")
    ap.add_argument("--history", action="store_true", help="Write history/ snapshot")
    ap.add_argument("--out", default="bench_results/report.md")
    ap.add_argument(
        "--upstream",
        default="bench_results/upstream/xray-core-v26.6.22.json",
        help="Fixed Xray-core upstream snapshot JSON (external reference)",
    )
    args = ap.parse_args()

    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    suites = [s.strip() for s in args.suites.split(",") if s.strip()]
    meta = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "sha": args.sha,
        "runner": args.runner,
        "go": args.go or "go",
    }

    md, rows, summary = assemble_report(
        results_dir, suites, meta, upstream_path=args.upstream
    )
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")

    (results_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    make_svg(rows, results_dir / "summary.svg")

    if args.history:
        hist = results_dir / "history"
        hist.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        short = (args.sha or "local")[:12]
        hist_json = hist / f"{stamp}-{short}.json"
        # compact history entry
        hist_payload = {
            "id": f"{stamp}-{short}",
            "generated": summary["generated"],
            "sha": summary["sha"],
            "runner": summary["runner"],
            "go": summary["go"],
            "verdict": summary["verdict"],
            "counts": summary["counts"],
            "metrics": [
                {
                    "key": f"{r['suite']}/{r['name']}",
                    "unit": r["unit"],
                    "current": r["current"],
                    "baseline": r["baseline"],
                    "upstream": r.get("upstream"),
                    "delta_pct": r["delta_pct"],
                    "delta_vs_upstream_pct": r.get("delta_vs_upstream_pct"),
                }
                for r in rows
            ],
        }
        hist_json.write_text(json.dumps(hist_payload, indent=2) + "\n", encoding="utf-8")
        # latest markdown snapshot (short)
        latest = [
            f"# Latest Bench Snapshot (`{short}`)",
            "",
            f"- Generated: {summary['generated']}",
            f"- Verdict: **{summary['verdict']}**",
            f"- 🟢 {summary['counts']['improved']} · ⚪ {summary['counts']['stable']} · "
            f"🔴 {summary['counts']['slower']} · 🆕 {summary['counts']['new']}",
            "",
            "See full formatted report in CI artifact / `bench_results/report.md`.",
            "",
            "| Benchmark | Current | Delta |",
            "|-----------|--------:|------:|",
        ]
        for r in sorted(rows, key=lambda x: (x["suite"], x["name"]))[:40]:
            d = "—" if r["delta_pct"] is None else f"{r['delta_pct']:+.1f}%"
            latest.append(
                f"| `{r['suite']}/{r['name']}` | {fmt_num(r['current'])} {r['unit']} | {d} |"
            )
        (hist / "latest.md").write_text("\n".join(latest) + "\n", encoding="utf-8")

    print(f"wrote {out} rows={len(rows)} verdict={summary['verdict']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
