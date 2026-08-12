package strmatcher_test

import (
	"strconv"
	"testing"

	"github.com/xtls/xray-core/common"
	. "github.com/xtls/xray-core/common/geodata/strmatcher"
)

// matcherGroup mirrors the production MatcherGroup interface for benchmark
// helpers (the production Add* helpers were removed).
type matcherGroup interface {
	Match(string) []uint32
	MatchAny(string) bool
}

func BenchmarkFullMatcher(b *testing.B) {
	b.Run("SimpleMatcherGroup------", func(b *testing.B) {
		benchmarkMatcherType(b, Full, func() matcherGroup {
			return new(SimpleMatcherGroup)
		})
	})
	b.Run("FullMatcherGroup--------", func(b *testing.B) {
		benchmarkMatcherType(b, Full, func() matcherGroup {
			return NewFullMatcherGroup()
		})
	})
	b.Run("ACAutomationMatcherGroup", func(b *testing.B) {
		benchmarkMatcherType(b, Full, func() matcherGroup {
			return NewACAutomatonMatcherGroup()
		})
	})
	b.Run("MphMatcherGroup---------", func(b *testing.B) {
		benchmarkMatcherType(b, Full, func() matcherGroup {
			return NewMphMatcherGroup()
		})
	})
}

func BenchmarkDomainMatcher(b *testing.B) {
	b.Run("SimpleMatcherGroup------", func(b *testing.B) {
		benchmarkMatcherType(b, Domain, func() matcherGroup {
			return new(SimpleMatcherGroup)
		})
	})
	b.Run("DomainMatcherGroup------", func(b *testing.B) {
		benchmarkMatcherType(b, Domain, func() matcherGroup {
			return NewDomainMatcherGroup()
		})
	})
	b.Run("ACAutomationMatcherGroup", func(b *testing.B) {
		benchmarkMatcherType(b, Domain, func() matcherGroup {
			return NewACAutomatonMatcherGroup()
		})
	})
	b.Run("MphMatcherGroup---------", func(b *testing.B) {
		benchmarkMatcherType(b, Domain, func() matcherGroup {
			return NewMphMatcherGroup()
		})
	})
}

func BenchmarkSubstrMatcher(b *testing.B) {
	b.Run("SimpleMatcherGroup------", func(b *testing.B) {
		benchmarkMatcherType(b, Substr, func() matcherGroup {
			return new(SimpleMatcherGroup)
		})
	})
	b.Run("SubstrMatcherGroup------", func(b *testing.B) {
		benchmarkMatcherType(b, Substr, func() matcherGroup {
			return new(SubstrMatcherGroup)
		})
	})
	b.Run("ACAutomationMatcherGroup", func(b *testing.B) {
		benchmarkMatcherType(b, Substr, func() matcherGroup {
			return NewACAutomatonMatcherGroup()
		})
	})
}

// Utility functions for benchmark

func benchmarkMatcherType(b *testing.B, t Type, ctor func() matcherGroup) {
	b.Run("Match", func(b *testing.B) {
		b.Run("Succ", func(b *testing.B) {
			benchmarkMatch(b, ctor(), map[Type]bool{t: true})
		})
		b.Run("Fail", func(b *testing.B) {
			benchmarkMatch(b, ctor(), map[Type]bool{t: false})
		})
	})
	b.Run("MatchAny", func(b *testing.B) {
		b.Run("Succ", func(b *testing.B) {
			benchmarkMatchAny(b, ctor(), map[Type]bool{t: true})
		})
		b.Run("Fail", func(b *testing.B) {
			benchmarkMatchAny(b, ctor(), map[Type]bool{t: false})
		})
	})
}

func benchmarkMatch(b *testing.B, g matcherGroup, enabledTypes map[Type]bool) {
	prepareMatchers(b, g, enabledTypes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.Match("0.example.com")
	}
}

func benchmarkMatchAny(b *testing.B, g matcherGroup, enabledTypes map[Type]bool) {
	prepareMatchers(b, g, enabledTypes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.MatchAny("0.example.com")
	}
}

// mustAddToGroup adds matcher to a concrete MatcherGroup via static dispatch,
// replacing the removed AddMatcherToGroup helper.
func mustAddToGroup(tb testing.TB, g matcherGroup, matcher Matcher, value uint32) {
	switch g := g.(type) {
	case *SimpleMatcherGroup:
		g.AddMatcher(matcher, value)
	case *FullMatcherGroup:
		g.AddFullMatcher(matcher.(FullMatcher), value)
	case *DomainMatcherGroup:
		g.AddDomainMatcher(matcher.(DomainMatcher), value)
	case *SubstrMatcherGroup:
		g.AddSubstrMatcher(matcher.(SubstrMatcher), value)
	case *ACAutomatonMatcherGroup:
		switch m := matcher.(type) {
		case FullMatcher:
			g.AddFullMatcher(m, value)
		case DomainMatcher:
			g.AddDomainMatcher(m, value)
		case SubstrMatcher:
			g.AddSubstrMatcher(m, value)
		default:
			tb.Fatalf("unsupported matcher %T for ACAutomatonMatcherGroup", matcher)
		}
	case *MphMatcherGroup:
		switch m := matcher.(type) {
		case FullMatcher:
			g.AddFullMatcher(m, value)
		case DomainMatcher:
			g.AddDomainMatcher(m, value)
		default:
			tb.Fatalf("unsupported matcher %T for MphMatcherGroup", matcher)
		}
	default:
		tb.Fatalf("unsupported MatcherGroup type %T", g)
	}
}

func prepareMatchers(tb testing.TB, g matcherGroup, enabledTypes map[Type]bool) {
	for matcherType, hasMatch := range enabledTypes {
		switch matcherType {
		case Domain:
			if hasMatch {
				mustAddToGroup(tb, g, DomainMatcher("example.com"), 0)
			}
			for i := 1; i < 1024; i++ {
				mustAddToGroup(tb, g, DomainMatcher(strconv.Itoa(i)+".example.com"), uint32(i))
			}
		case Full:
			if hasMatch {
				mustAddToGroup(tb, g, FullMatcher("0.example.com"), 0)
			}
			for i := 1; i < 64; i++ {
				mustAddToGroup(tb, g, FullMatcher(strconv.Itoa(i)+".example.com"), uint32(i))
			}
		case Substr:
			if hasMatch {
				mustAddToGroup(tb, g, SubstrMatcher("example.com"), 0)
			}
			for i := 1; i < 4; i++ {
				mustAddToGroup(tb, g, SubstrMatcher(strconv.Itoa(i)+".example.com"), uint32(i))
			}
		}
	}
	if g, ok := g.(buildable); ok {
		common.Must(g.Build())
	}
}

type buildable interface {
	Build() error
}
