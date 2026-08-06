# Branch policy

| Branch | Tip role |
|--------|----------|
| **main** | Default product line. Bray fully-hardened XHTTP + REALITY Amortize stack (historical feature branch name: `Bray-V2`, Waves 1–7) plus Bray-only session MAC / packet-up perf. **Not upstream-compatible as a peer.** |
| **v1** | Frozen snapshot of the previous `main` line before promotion. Use for rollback or A/B comparison. |

## Operator notes

- Clone defaults to `main`.
- Release / Docker / tests CI tracks `main`.
- Feature docs under `docs/bray-v2-*.md` describe the stack that now lives on `main`; keep filenames for link stability.
- Do not reopen large default-behavior changes on `v1`; backport only critical security fixes if needed.

## Local promotion commands (if refs need fixing)

```powershell
cd D:\UGit\Bray-Core
# old main tip -> v1, Bray-V2 tip -> main
git branch v1 54eccc0a
git branch -f main 7751faef
git checkout main
git push -u origin v1
git push origin main
# optional: delete remote feature branch after verify
# git push origin --delete Bray-V2
```
