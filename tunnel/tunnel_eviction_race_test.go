package tunnel

// Coverage for the PrivilegeNewestRegistration eviction-race fixes beyond
// Edouard's tunnel_concurrent_test.go: the post-record stale side-effect
// interleaving, the deleteClient reentrancy + subscription-leak boundary, the
// deleteTarget strand-prune belt, duplicate-subscriber-ADD suppression,
// best-effort delete-handler failure, and clientTargets(nil) lock coverage.

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tpb "github.com/openconfig/grpctunnel/proto/tunnel"
)

func mustResolve(t *testing.T, addr string) net.Addr {
	t.Helper()
	a, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		t.Fatalf("resolve %s: %v", addr, err)
	}
	return a
}

func evictionTestKey() (Target, *tpb.Target) {
	key := Target{ID: "dev1", Type: "GNMI_GNOI"}
	return key, &tpb.Target{Target: key.ID, TargetType: key.Type, Op: tpb.Target_ADD}
}

// countTargetADDs returns how many target-ADD RegisterOps were sent on rs.
func countTargetADDs(rs *regSafeStream, key Target) int {
	rts, ok := rs.regStream.(*registerTestStream)
	if !ok {
		return -1
	}
	n := 0
	for _, op := range rts.streamSend {
		to := op.GetTarget()
		if to == nil {
			continue
		}
		if to.GetOp() == tpb.Target_ADD && to.GetTarget() == key.ID && to.GetTargetType() == key.Type {
			n++
		}
	}
	return n
}

// 1. Post-record eviction (the key interleaving): a client that recorded itself
// and is then evicted must not fire the accept ack / AddTargetHandler / subscriber
// ADD as a non-owner. Drives the record and the side effects around a barrier so
// the winner evicts in between.
func TestEvictionAfterRecordSkipsStaleSideEffects(t *testing.T) {
	victim := mustResolve(t, "127.0.0.1:45300")
	winner := mustResolve(t, "127.0.0.1:45301")
	key, tgt := evictionTestKey()

	var addCount int32
	s, err := NewServer(ServerConfig{
		PrivilegeNewestRegistration: true,
		AddTargetHandler:            func(Target) error { atomic.AddInt32(&addCount, 1); return nil },
		DeleteTargetHandler:         func(Target) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	victimRS := newConcurrentClientStream()
	if err := s.addClient(victim, victimRS); err != nil {
		t.Fatalf("addClient victim: %v", err)
	}
	if err := s.addClient(winner, newConcurrentClientStream()); err != nil {
		t.Fatalf("addClient winner: %v", err)
	}

	recorded := make(chan struct{})
	evicted := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ev, err := s.addTargetToMap(victim, key)
		if err != nil || ev != nil {
			t.Errorf("victim addTargetToMap: evicted=%v err=%v, want clean add", ev, err)
		}
		if !s.addTargetToClient(victim, key) {
			t.Errorf("victim addTargetToClient returned false, want recorded (it owned t here)")
		}
		close(recorded)
		<-evicted
		// Victim resumes its post-record side effects; the recheck must bail.
		if err := s.finishAddTarget(victim, victimRS, tgt, key, ev == nil); err == nil {
			t.Errorf("victim finishAddTarget succeeded, want re-homed error")
		}
	}()

	<-recorded
	if err := s.addTarget(winner, tgt); err != nil {
		t.Fatalf("winner addTarget (evict): %v", err)
	}
	close(evicted)
	<-done

	if got := s.clientFromTarget(key); got != winner {
		t.Errorf("owner = %v, want winner %v", got, winner)
	}
	if _, ok := s.clientTargets(victim)[key]; ok {
		t.Errorf("victim still holds the target after eviction")
	}
	if n := atomic.LoadInt32(&addCount); n != 1 {
		t.Errorf("AddTargetHandler fired %d times, want 1 (winner only, not the evicted victim)", n)
	}
	checkOwnership(t, s, []net.Addr{victim, winner}, key, "post-record-eviction")
}

// 2. deleteClient must not reentrant-deadlock when a stranded target is still
// recorded in the client set at teardown.
func TestDeleteClientStrandedTargetNoDeadlock(t *testing.T) {
	a := mustResolve(t, "127.0.0.1:45310")
	key, _ := evictionTestKey()
	s, err := NewServer(ServerConfig{PrivilegeNewestRegistration: true})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := s.addClient(a, newConcurrentClientStream()); err != nil {
		t.Fatalf("addClient: %v", err)
	}
	// Strand: recorded in the client set, absent from rTargets.
	s.clients[a].targets[key] = struct{}{}

	done := make(chan struct{})
	go func() { s.deleteClient(a); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("deleteClient hung on a stranded target (reentrant cmu deadlock)")
	}
	if !s.clientInfo(a).IsZero() {
		t.Error("deleteClient did not remove the client")
	}
}

// 3. deleteTarget must prune the client set even when the target is missing from
// rTargets (returns an error but still cleans up).
func TestDeleteTargetPrunesStrandedClientSet(t *testing.T) {
	a := mustResolve(t, "127.0.0.1:45311")
	key, tgt := evictionTestKey()
	s, err := NewServer(ServerConfig{PrivilegeNewestRegistration: true})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := s.addClient(a, newConcurrentClientStream()); err != nil {
		t.Fatalf("addClient: %v", err)
	}
	s.clients[a].targets[key] = struct{}{}
	_ = s.deleteTarget(a, tgt, false) // !ok error expected, but it must still prune
	if _, ok := s.clientTargets(a)[key]; ok {
		t.Error("deleteTarget left the stranded target in the client set")
	}
}

// 4. Subscription cleanup belongs to client teardown, not target teardown.
func TestDeleteClientClearsSubscription(t *testing.T) {
	a := mustResolve(t, "127.0.0.1:45312")
	key, _ := evictionTestKey()
	s, err := NewServer(ServerConfig{PrivilegeNewestRegistration: true})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := s.addClient(a, newConcurrentClientStream()); err != nil {
		t.Fatalf("addClient: %v", err)
	}
	if err := s.addSubscription(a, key.Type); err != nil {
		t.Fatalf("addSubscription: %v", err)
	}
	s.deleteClient(a)
	s.smu.RLock()
	_, ok := s.sub[a]
	s.smu.RUnlock()
	if ok {
		t.Error("deleteClient did not clear s.sub[a]")
	}
}

// 5. After an eviction empties the evicted client's target set, its disconnect
// must still clean the subscription, and a later sendUpdates must not try to
// reach the dead client.
func TestSubscriptionClearedAfterEviction(t *testing.T) {
	a := mustResolve(t, "127.0.0.1:45320")
	b := mustResolve(t, "127.0.0.1:45321")
	key, tgt := evictionTestKey()
	s, err := NewServer(ServerConfig{PrivilegeNewestRegistration: true})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := s.addClient(a, newConcurrentClientStream()); err != nil {
		t.Fatalf("addClient a: %v", err)
	}
	if err := s.addClient(b, newConcurrentClientStream()); err != nil {
		t.Fatalf("addClient b: %v", err)
	}
	if err := s.addSubscription(a, key.Type); err != nil {
		t.Fatalf("addSubscription: %v", err)
	}
	if err := s.addTarget(a, tgt); err != nil {
		t.Fatalf("addTarget a: %v", err)
	}
	if err := s.addTarget(b, tgt); err != nil { // evicts a
		t.Fatalf("addTarget b (evict): %v", err)
	}
	s.deleteTargets(a, false)
	s.deleteClient(a)

	s.smu.RLock()
	_, ok := s.sub[a]
	s.smu.RUnlock()
	if ok {
		t.Error("s.sub[a] leaked after eviction + disconnect")
	}
	if err := s.sendUpdates(key, false); err != nil {
		t.Errorf("sendUpdates after eviction tried to reach the dead subscriber: %v", err)
	}
}

// 6. On a newest-wins re-home the target-name stays continuously present, so a
// subscriber must not receive a second ADD.
func TestNoDuplicateSubscriberADDOnEviction(t *testing.T) {
	a := mustResolve(t, "127.0.0.1:45330")
	b := mustResolve(t, "127.0.0.1:45331")
	c := mustResolve(t, "127.0.0.1:45332")
	key, tgt := evictionTestKey()
	s, err := NewServer(ServerConfig{PrivilegeNewestRegistration: true})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	cRS := newConcurrentClientStream()
	if err := s.addClient(a, newConcurrentClientStream()); err != nil {
		t.Fatalf("addClient a: %v", err)
	}
	if err := s.addClient(b, newConcurrentClientStream()); err != nil {
		t.Fatalf("addClient b: %v", err)
	}
	if err := s.addClient(c, cRS); err != nil {
		t.Fatalf("addClient c: %v", err)
	}
	if err := s.addSubscription(c, key.Type); err != nil {
		t.Fatalf("addSubscription: %v", err)
	}
	if err := s.addTarget(a, tgt); err != nil { // subscriber c gets one ADD
		t.Fatalf("addTarget a: %v", err)
	}
	if err := s.addTarget(b, tgt); err != nil { // evicts a → no second ADD
		t.Fatalf("addTarget b (evict): %v", err)
	}
	if n := countTargetADDs(cRS, key); n != 1 {
		t.Errorf("subscriber received %d target ADDs, want 1 (no duplicate on re-home)", n)
	}
}

// 7. DeleteTargetHandler failure during eviction is intentional best-effort: the
// new owner is still stood up and the error is surfaced on ErrorChan (receiver
// started before the eviction, since sendError is non-blocking on an unbuffered
// channel).
func TestEvictionDeleteHandlerFailureBestEffort(t *testing.T) {
	a := mustResolve(t, "127.0.0.1:45340")
	b := mustResolve(t, "127.0.0.1:45341")
	key, tgt := evictionTestKey()
	sentinel := errors.New("delete handler boom")
	var addCount int32
	s, err := NewServer(ServerConfig{
		PrivilegeNewestRegistration: true,
		AddTargetHandler:            func(Target) error { atomic.AddInt32(&addCount, 1); return nil },
		DeleteTargetHandler:         func(Target) error { return sentinel },
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := s.addClient(a, newConcurrentClientStream()); err != nil {
		t.Fatalf("addClient a: %v", err)
	}
	if err := s.addClient(b, newConcurrentClientStream()); err != nil {
		t.Fatalf("addClient b: %v", err)
	}

	errs := make(chan error, 8)
	drainDone := make(chan struct{})
	defer close(drainDone)
	go func() {
		for {
			select {
			case e := <-s.ErrorChan():
				select {
				case errs <- e:
				default:
				}
			case <-drainDone:
				return
			}
		}
	}()
	time.Sleep(50 * time.Millisecond) // let the drainer park on ErrorChan

	if err := s.addTarget(a, tgt); err != nil {
		t.Fatalf("addTarget a: %v", err)
	}
	if err := s.addTarget(b, tgt); err != nil { // evicts a; DeleteTargetHandler fails
		t.Fatalf("addTarget b (evict): %v", err)
	}

	if got := s.clientFromTarget(key); got != b {
		t.Errorf("owner = %v, want b %v (eviction must complete despite delete-handler failure)", got, b)
	}
	if _, ok := s.clientTargets(b)[key]; !ok {
		t.Error("b's client set is missing the target")
	}
	if n := atomic.LoadInt32(&addCount); n != 2 {
		t.Errorf("AddTargetHandler fired %d times, want 2 (a then b)", n)
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case e := <-errs:
			if strings.Contains(e.Error(), sentinel.Error()) {
				return
			}
		case <-deadline:
			t.Fatal("delete-handler sentinel error was not observed on ErrorChan")
		}
	}
}

// 8. clientTargets(nil) must read rTargets under tmu, not cmu. Under -race this
// fails if it uses the wrong lock while another goroutine mutates rTargets.
func TestClientTargetsNilLockRace(t *testing.T) {
	_, tgt := evictionTestKey()
	s, err := NewServer(ServerConfig{PrivilegeNewestRegistration: true})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	addrs := make([]net.Addr, 4)
	for i := range addrs {
		addrs[i] = mustResolve(t, fmt.Sprintf("127.0.0.1:%d", 45350+i))
		if err := s.addClient(addrs[i], newConcurrentClientStream()); err != nil {
			t.Fatalf("addClient: %v", err)
		}
	}

	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				_ = s.clientTargets(nil)
			}
		}
	}()

	var wg sync.WaitGroup
	for _, a := range addrs {
		wg.Add(1)
		go func(a net.Addr) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				_ = s.addTarget(a, tgt)
				_ = s.deleteTarget(a, tgt, false)
			}
		}(a)
	}
	wg.Wait()
	close(done)
}
