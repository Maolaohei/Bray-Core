package main

// Target (repository) management: builds isolated git worktrees for each
// target under .gotmp/benchcmp/, injects harness-owned bench files, and tears
// everything down afterwards. Nothing in the main worktree is touched.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// repoRoot is the main repository root (where the harness lives).
var repoRoot string

func init() {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	repoRoot = wd
}

func gitCmd(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return strings.TrimSpace(string(out)), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

func worktreeRoot(name string) string {
	return filepath.Join(repoRoot, ".gotmp", "benchcmp", name)
}

// prepareTargets materializes the requested targets and injects bench files.
// The returned cleanup func removes the worktrees (and any injected files).
func prepareTargets(suites []*benchSuite, targets []string, upstreamPath string) (map[string]*benchTarget, func(), error) {
	targets = dedupe(targets)
	t := make(map[string]*benchTarget, len(targets))
	for _, name := range targets {
		var dir string
		switch name {
		case "bray":
			dir = worktreeRoot("bray")
			cleanup, err := ensureWorktree(dir, "HEAD")
			if err != nil {
				return nil, nil, err
			}
			_ = cleanup
		case "upstream":
			dir = worktreeRoot("upstream")
			if upstreamPath != "" {
				abs, err := filepath.Abs(upstreamPath)
				if err != nil {
					return nil, nil, err
				}
				dir = abs
				if _, err := gitCmd(dir, "rev-parse", "--git-dir"); err != nil {
					return nil, nil, fmt.Errorf("--upstream-path %s is not a git repo: %w", abs, err)
				}
			} else {
				if _, err := ensureWorktree(dir, "upstream/main"); err != nil {
					return nil, nil, err
				}
			}
		default:
			return nil, nil, fmt.Errorf("unknown target %q (available: bray, upstream)", name)
		}
		c, err := gitCmd(dir, "rev-parse", "--short", "HEAD")
		if err != nil {
			return nil, nil, err
		}
		bt := &benchTarget{Name: name, Dir: dir, Commit: c}
		if name == "bray" {
			// Bray's session wire mode is fail-closed: without the shared
			// secret header the packet-up/stream-up servers reject the bench.
			bt.Session = "x-bray-session-secret=bench-test-secret"
		}
		t[name] = bt
	}

	// Inject bench files (and drop conflicting repo copies) per suite per target.
	for _, s := range suites {
		for _, name := range targets {
			bt := t[name]
			for _, rel := range s.Remove[name] {
				p := filepath.Join(bt.Dir, filepath.FromSlash(rel))
				if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
					return nil, nil, fmt.Errorf("remove %s: %w", p, err)
				}
			}
			for _, rel := range s.Inject[name] {
				src := filepath.Join(repoRoot, "tools", "benchcompare", "testdata", filepath.FromSlash(rel))
				dst := filepath.Join(bt.Dir, filepath.FromSlash(s.Pkg), "zz_benchcmp_"+filepath.Base(rel))
				data, err := os.ReadFile(src)
				if err != nil {
					return nil, nil, err
				}
				if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
					return nil, nil, err
				}
				if err := os.WriteFile(dst, data, 0o644); err != nil {
					return nil, nil, err
				}
			}
		}
	}
	return t, func() { teardownTargets(t, targets) }, nil
}

// ensureWorktree creates (or reuses) a detached worktree at dir for ref.
// Worktrees are pre-created and pre-populated by an earlier `git worktree add`
// so that a first run cannot race; leftovers from a previous run are removed
// first so the result is always a clean checkout.
func ensureWorktree(dir, ref string) (func(), error) {
	// Remove stale worktree if present (in case a previous run crashed).
	if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
		_, _ = gitCmd(repoRoot, "worktree", "remove", "--force", dir)
	}
	if _, err := os.Stat(dir); err == nil {
		_ = os.RemoveAll(dir)
	}
	if _, err := gitCmd(repoRoot, "worktree", "add", "--detach", dir, ref); err != nil {
		return nil, fmt.Errorf("worktree add %s @ %s: %w", dir, ref, err)
	}
	// Populate submodules (REALITY etc.) inside the fresh worktree; some git
	// versions lack `worktree add --recurse-submodules`.
	if _, err := gitCmd(dir, "submodule", "update", "--init", "--recursive"); err != nil {
		return nil, fmt.Errorf("submodule update in %s: %w", dir, err)
	}
	return func() {}, nil
}

func teardownTargets(t map[string]*benchTarget, targets []string) {
	for _, name := range targets {
		bt := t[name]
		if bt == nil {
			continue
		}
		// Only tear down worktrees the harness created (under .gotmp/benchcmp),
		// never a user-supplied --upstream-path checkout.
		prefix := filepath.Join(repoRoot, ".gotmp", "benchcmp")
		if strings.HasPrefix(bt.Dir, prefix) {
			_, _ = gitCmd(repoRoot, "worktree", "remove", "--force", bt.Dir)
			_ = os.RemoveAll(bt.Dir)
		}
	}
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
