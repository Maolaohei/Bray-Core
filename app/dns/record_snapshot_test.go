package dns

import (
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"golang.org/x/net/dns/dnsmessage"
)

// TestFindRecordsSnapshotSurvivesCleanupMutation verifies that a concurrent
// reader of findRecords() is not affected when cleanup clears/deletes the map
// entry. This used to race with recordObjPool reuse and could return another
// domain's IPs (cross-host TLS cert mismatch).
func TestFindRecordsSnapshotSurvivesCleanupMutation(t *testing.T) {
	c := NewCacheController("snap", false, false, 0)

	githubIP := net.IP{20, 205, 243, 166}
	googleIP := net.IP{142, 250, 190, 14}

	c.Lock()
	c.ips["github.com."] = &record{
		A: &IPRecord{
			IP:     []net.IP{githubIP},
			Expire: time.Now().Add(time.Hour),
			RCode:  dnsmessage.RCodeSuccess,
		},
	}
	c.Unlock()

	snap := c.findRecords("github.com.")
	if snap == nil || snap.A == nil {
		t.Fatal("expected snapshot")
	}

	// Simulate cleanup/update: replace live map entry with different data
	// and clear the old object as the old pool put() would have done.
	c.Lock()
	old := c.ips["github.com."]
	c.ips["github.com."] = &record{
		A: &IPRecord{
			IP:     []net.IP{googleIP},
			Expire: time.Now().Add(time.Hour),
			RCode:  dnsmessage.RCodeSuccess,
		},
	}
	old.A = nil
	old.AAAA = nil
	c.ips["www.google.com."] = old
	old.A = &IPRecord{
		IP:     []net.IP{googleIP},
		Expire: time.Now().Add(time.Hour),
		RCode:  dnsmessage.RCodeSuccess,
	}
	c.Unlock()

	ips, _, err := snap.A.getIPs()
	if err != nil || len(ips) != 1 || !ips[0].Equal(githubIP) {
		t.Fatalf("snapshot mutated to wrong IP: ips=%v err=%v", ips, err)
	}
}

// TestCacheControllerNoRecordPoolRace stress-tests concurrent find + update + cleanup.
func TestCacheControllerNoRecordPoolRace(t *testing.T) {
	c := NewCacheController("race", false, true, 30)
	domains := []string{"github.com.", "www.google.com.", "chat.openai.com."}
	ips := []net.IP{
		{20, 205, 243, 166},
		{142, 250, 190, 14},
		{104, 18, 32, 111},
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				req := &dnsRequest{
					reqType: dnsmessage.TypeA,
					domain:  domains[i],
					start:   time.Now(),
				}
				c.updateRecord(req, &IPRecord{
					IP:     []net.IP{ips[i]},
					Expire: time.Now().Add(50 * time.Millisecond),
					RCode:  dnsmessage.RCodeSuccess,
				})
			}
		}(i)
	}

	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i, d := range domains {
					rec := c.findRecords(d)
					if rec == nil || rec.A == nil {
						continue
					}
					got, _, err := rec.A.getIPs()
					if err != nil || len(got) == 0 {
						continue
					}
					if !got[0].Equal(ips[i]) {
						t.Errorf("domain %s got IP %v want %v", d, got[0], ips[i])
						return
					}
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = c.CacheCleanup()
			time.Sleep(time.Millisecond)
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}
