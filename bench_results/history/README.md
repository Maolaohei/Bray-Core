# Bench history

CI (`Benchmark Tracking`) writes one JSON snapshot per successful format step:

- `YYYYMMDDTHHMMSSZ-<sha12>.json` — counts + per-metric current/baseline/delta
- `latest.md` — short human summary of the last run

Use these for performance evolution curves. Do **not** compare H2 vs H2C vs packet-up as a single series.

CI suites include **`xhttp_core`** (Advantage) plus regression suites (`xmux,happy,warmup,vless,buf`).
Reports ship an **Advantage Highlights** section separate from regression Summary counts.

Local:

```bash
python scripts/format_bench_report.py --history --suites xhttp_core,xmux,happy,warmup,vless,buf --sha "$(git rev-parse --short HEAD)"
```
