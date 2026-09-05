package dns

import (
	"context"
	"encoding/binary"
	stderrors "errors"
	"math"
	"strings"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	dns_feature "github.com/xtls/xray-core/features/dns"

	"golang.org/x/net/dns/dnsmessage"
)

// Fqdn normalizes domain make sure it ends with '.'
// case-sensitive
func Fqdn(domain string) string {
	if len(domain) > 0 && strings.HasSuffix(domain, ".") {
		return domain
	}
	return domain + "."
}

// isContextDoneErr reports cancel/deadline errors expected when a DNS query is
// abandoned early (sibling dual-stack answer, outer QueryIP timeout, etc.).
func isContextDoneErr(err error) bool {
	if err == nil {
		return false
	}
	if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// net/http and url.Error often wrap cancel as "...: context canceled"
	s := err.Error()
	return strings.Contains(s, "context canceled") || strings.Contains(s, "context deadline exceeded")
}

type record struct {
	A    *IPRecord
	AAAA *IPRecord
}

// IPRecord is a cacheable item for a resolved domain
type IPRecord struct {
	ReqID     uint16
	IP        []net.IP
	Expire    time.Time
	RCode     dnsmessage.RCode
	RawHeader *dnsmessage.Header
	// Question is the echoed question name (fqdn with trailing dot), used by
	// callers to verify the response belongs to the request (RFC 5452).
	Question string
}

func (r *IPRecord) getIPs() ([]net.IP, int32, error) {
	if r == nil {
		return nil, 0, errRecordNotFound
	}

	untilExpire := time.Until(r.Expire).Seconds()
	ttl := int32(math.Ceil(untilExpire))

	if r.RCode != dnsmessage.RCodeSuccess {
		return nil, ttl, dns_feature.RCodeError(r.RCode)
	}
	if len(r.IP) == 0 {
		return nil, ttl, dns_feature.ErrEmptyResponse
	}

	return r.IP, ttl, nil
}

var errRecordNotFound = errors.New("record not found")

// Razor note (2026-09-05): the former optResourcePool / messagePool /
// dnsRequestPool sync.Pools are gone. DNS query rate is far too low to make
// pooling measurable, yet the pools twice produced real bugs: the OPT body
// was shared across queries (DNS 专项 D1 race, fixed by a deep-copy that
// nullified the pool's benefit) and early pool-returns caused UAF in DoH.
// Plain per-query allocation keeps the lifetime owned by GC.

type dnsRequest struct {
	reqType dnsmessage.Type
	domain  string
	start   time.Time
	expire  time.Time
	msg     *dnsmessage.Message
}

func genEDNS0Options(clientIP net.IP, padding int) *dnsmessage.Resource {
	if len(clientIP) == 0 && padding == 0 {
		return nil
	}

	const EDNS0SUBNET = 0x8
	const EDNS0PADDING = 0xc

	opt := &dnsmessage.Resource{
		Body: &dnsmessage.OPTResource{},
	}
	opt.Header.SetEDNS0(1350, 0xfe00, true)
	body := opt.Body.(*dnsmessage.OPTResource)

	if len(clientIP) != 0 {
		var netmask int
		var family uint16

		if len(clientIP) == 4 {
			family = 1
			netmask = 24 // 24 for IPV4, 96 for IPv6
		} else {
			family = 2
			netmask = 96
		}

		b := make([]byte, 4)
		binary.BigEndian.PutUint16(b[0:], family)
		b[2] = byte(netmask)
		b[3] = 0
		switch family {
		case 1:
			ip := clientIP.To4().Mask(net.CIDRMask(netmask, net.IPv4len*8))
			needLength := (netmask + 8 - 1) / 8 // division rounding up
			b = append(b, ip[:needLength]...)
		case 2:
			ip := clientIP.Mask(net.CIDRMask(netmask, net.IPv6len*8))
			needLength := (netmask + 8 - 1) / 8 // division rounding up
			b = append(b, ip[:needLength]...)
		}

		body.Options = append(body.Options,
			dnsmessage.Option{
				Code: EDNS0SUBNET,
				Data: b,
			})
	}

	if padding != 0 {
		body.Options = append(body.Options,
			dnsmessage.Option{
				Code: EDNS0PADDING,
				Data: make([]byte, padding),
			})
	}

	return opt
}

// buildReqMsgs builds one A and/or AAAA query message per requested family.
// The returned messages own their payload (fresh allocation); callers never
// share or recycle them, so no release discipline is required.
func buildReqMsgs(domain string, option dns_feature.IPOption, reqIDGen func() uint16, reqOpts *dnsmessage.Resource) ([]*dnsRequest, error) {
	name, err := dnsmessage.NewName(domain)
	if err != nil {
		return nil, err
	}

	qA := dnsmessage.Question{
		Name:  name,
		Type:  dnsmessage.TypeA,
		Class: dnsmessage.ClassINET,
	}

	qAAAA := dnsmessage.Question{
		Name:  name,
		Type:  dnsmessage.TypeAAAA,
		Class: dnsmessage.ClassINET,
	}

	var reqs []*dnsRequest
	now := time.Now()

	if option.IPv4Enable {
		msg := &dnsmessage.Message{
			Header: dnsmessage.Header{
				ID:               reqIDGen(),
				RecursionDesired: true,
			},
			Questions: []dnsmessage.Question{qA},
		}
		if reqOpts != nil {
			// reqOpts is freshly allocated per query and immutable after
			// construction — sharing its Body pointer is safe.
			msg.Additionals = append(msg.Additionals, *reqOpts)
		}
		reqs = append(reqs, &dnsRequest{
			reqType: dnsmessage.TypeA,
			domain:  domain,
			start:   now,
			msg:     msg,
		})
	}

	if option.IPv6Enable {
		msg := &dnsmessage.Message{
			Header: dnsmessage.Header{
				ID:               reqIDGen(),
				RecursionDesired: true,
			},
			Questions: []dnsmessage.Question{qAAAA},
		}
		if reqOpts != nil {
			msg.Additionals = append(msg.Additionals, *reqOpts)
		}
		reqs = append(reqs, &dnsRequest{
			reqType: dnsmessage.TypeAAAA,
			domain:  domain,
			start:   now,
			msg:     msg,
		})
	}

	return reqs, nil
}

// negativeCacheTTL bounds negative caching (NXDOMAIN / empty responses) so a
// poisoned or broken answer cannot block a domain for minutes.
const negativeCacheTTL = 60

// maxRecordedIPs caps the number of A/AAAA addresses collected per response
// (memory bound against hostile/broken servers).
const maxRecordedIPs = 32

// responseMatchesRequest verifies the echoed question matches the request
// domain (cache-poisoning guard, RFC 5452). A response whose question is
// missing or does not match is discarded by callers before it can populate
// the cache.
func responseMatchesRequest(domain string, ipRec *IPRecord) bool {
	if ipRec == nil || ipRec.Question == "" {
		return false
	}
	return strings.EqualFold(strings.TrimSuffix(ipRec.Question, "."), strings.TrimSuffix(domain, "."))
}

// parseResponse parses DNS answers from the returned payload
func parseResponse(payload []byte) (*IPRecord, error) {
	var parser dnsmessage.Parser
	h, err := parser.Start(payload)
	if err != nil {
		return nil, errors.New("failed to parse DNS response").Base(err).AtWarning()
	}
	// Read the echoed question (if present) so callers can verify the response
	// belongs to the request; skip any additional questions.
	var parsedQuestion string
	if qh, err := parser.Question(); err == nil {
		parsedQuestion = qh.Name.String()
	} else if err != dnsmessage.ErrSectionDone {
		return nil, errors.New("failed to read question in DNS response").Base(err).AtWarning()
	}
	for {
		if _, err := parser.Question(); err != nil {
			break
		}
	}

	now := time.Now()
	ipRecord := &IPRecord{
		ReqID:     h.ID,
		RCode:     h.RCode,
		RawHeader: &h,
		Question:  parsedQuestion,
	}
	defer func() {
		// Negative caching: default 60s (was 300s) so a poisoned NXDOMAIN or
		// empty response cannot block a domain for five minutes.
		if ipRecord.Expire.IsZero() {
			ipRecord.Expire = now.Add(time.Second * negativeCacheTTL)
		}
	}()

L:
	for {
		ah, err := parser.AnswerHeader()
		if err != nil {
			if err != dnsmessage.ErrSectionDone {
				errors.LogInfoInner(context.Background(), err, "failed to parse answer section for domain: ", ah.Name.String())
			}
			break
		}

		ttl := ah.TTL
		if ttl == 0 {
			// RFC 1035: TTL 0 means "do not cache". Serve the record once and
			// mark it immediately expired so it is never reused.
			ipRecord.Expire = now
		}
		if ttl > 86400 {
			// Cap absurd TTLs (malicious/broken servers) at one day.
			ttl = 86400
		}
		expire := now.Add(time.Duration(ttl) * time.Second)
		if ipRecord.Expire.IsZero() || ipRecord.Expire.After(expire) {
			ipRecord.Expire = expire
		}

		switch ah.Type {
		case dnsmessage.TypeA:
			ans, err := parser.AResource()
			if err != nil {
				errors.LogInfoInner(context.Background(), err, "failed to parse A record for domain: ", ah.Name)
				break L
			}
			if len(ipRecord.IP) >= maxRecordedIPs {
				break L
			}
			ipRecord.IP = append(ipRecord.IP, net.IPAddress(ans.A[:]).IP())
		case dnsmessage.TypeAAAA:
			ans, err := parser.AAAAResource()
			if err != nil {
				errors.LogInfoInner(context.Background(), err, "failed to parse AAAA record for domain: ", ah.Name)
				break L
			}
			newIP := net.IPAddress(ans.AAAA[:]).IP()
			if len(newIP) == net.IPv6len {
				if len(ipRecord.IP) >= maxRecordedIPs {
					break L
				}
				ipRecord.IP = append(ipRecord.IP, newIP)
			}
		default:
			if err := parser.SkipAnswer(); err != nil {
				errors.LogInfoInner(context.Background(), err, "failed to skip answer")
				break L
			}
			continue
		}
	}

	return ipRecord, nil
}

// toDnsContext create a new background context with parent inbound, session and dns log
func toDnsContext(ctx context.Context, addr string) context.Context {
	dnsCtx := core.ToBackgroundDetachedContext(ctx)
	if inbound := session.InboundFromContext(ctx); inbound != nil {
		dnsCtx = session.ContextWithInbound(dnsCtx, inbound)
	}
	dnsCtx = session.ContextWithContent(dnsCtx, session.ContentFromContext(ctx))
	dnsCtx = log.ContextWithAccessMessage(dnsCtx, &log.AccessMessage{
		From:   "DNS",
		To:     addr,
		Status: log.AccessAccepted,
		Reason: "",
	})
	return dnsCtx
}
