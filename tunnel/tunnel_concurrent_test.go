package tunnel

import (
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	tpb "github.com/openconfig/grpctunnel/proto/tunnel"
)

// Regression coverage for the PrivilegeNewestRegistration eviction races:
//
//   #1/#2  addTarget claims the target in rTargets (addTargetToMap) and then,
//          with no lock held and no ownership re-check, unconditionally records
//          it in its own client set (addTargetToClient). A client that clean-adds
//          and is preempted before addTargetToClient can be evicted meanwhile and
//          then strand the target in its own set. On its later disconnect,
//          deleteTargetFromMap returns the not-registered error, deleteTarget
//          returns early leaving the target stranded, and deleteClient (holding
//          cmu.Lock) calls deleteTarget -> clientInfo -> cmu.RLock -> a reentrant
//          self-deadlock that wedges cmu server-wide.
//   #3     the same non-atomicity lets a losing client run AddTargetHandler after
//          the true owner and leaves rTargets diverged from two clients' sets.
//
// The single invariant these tests assert is: whenever the server is quiescent,
// the client that owns a target in rTargets is the one and only client whose
// per-client target set contains it. A strand/divergence breaks it. The teardown
// watchdog independently catches the #2 deadlock.

func newConcurrentClientStream() *regSafeStream {
	return &regSafeStream{regStream: &registerTestStream{maxSends: 1 << 20}}
}

// checkOwnership reports whether rTargets and the per-client target sets agree on
// who owns key. On a violation it fails the test (non-fatally, so the caller can
// continue to exercise the resulting deadlock) and returns false.
func checkOwnership(t *testing.T, s *Server, addrs []net.Addr, key Target, ctx string) bool {
	t.Helper()
	owner := s.clientFromTarget(key)
	var holders []net.Addr
	for _, a := range addrs {
		if _, ok := s.clientTargets(a)[key]; ok {
			holders = append(holders, a)
		}
	}
	switch {
	case owner == nil && len(holders) != 0:
		t.Errorf("%s: rTargets has no owner for %v but client sets still hold it: %v (finding #1/#3)", ctx, key, holders)
		return false
	case owner != nil && (len(holders) != 1 || holders[0] != owner):
		t.Errorf("%s: divergent ownership for %v: rTargets owner=%v, client-set holders=%v (finding #1/#3)", ctx, key, owner, holders)
		return false
	}
	return true
}

// teardownWithWatchdog mirrors Register's deferred cleanup (LIFO: deleteTargets
// then deleteClient) for every addr concurrently, and fails if it does not finish
// within d — the signature of the finding #2 reentrant-cmu deadlock.
func teardownWithWatchdog(t *testing.T, s *Server, addrs []net.Addr, d time.Duration, ctx string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for _, a := range addrs {
			wg.Add(1)
			go func(a net.Addr) {
				defer wg.Done()
				s.deleteTargets(a, false)
				s.deleteClient(a)
			}(a)
		}
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Errorf("%s: DEADLOCK — client teardown hung >%s (reentrant cmu via deleteClient->deleteTarget->clientInfo, finding #2)", ctx, d)
	}
}

// TestPrivilegeNewestRegistrationEvictionStrandDeadlock deterministically forces
// the strand interleaving so #1/#2/#3 are exercised on every run. It models
// "addTarget was preempted between addTargetToMap (~tunnel.go:494) and
// addTargetToClient (~:520)" by driving the victim's two real sub-steps around a
// barrier while the winner evicts it in between.
func TestPrivilegeNewestRegistrationEvictionStrandDeadlock(t *testing.T) {
	victim, err := net.ResolveTCPAddr("tcp", "127.0.0.1:45200")
	if err != nil {
		t.Fatalf("resolve victim: %v", err)
	}
	winner, err := net.ResolveTCPAddr("tcp", "127.0.0.1:45201")
	if err != nil {
		t.Fatalf("resolve winner: %v", err)
	}
	key := Target{ID: "dev1", Type: "GNMI_GNOI"}
	tgt := &tpb.Target{Target: key.ID, TargetType: key.Type, Op: tpb.Target_ADD}

	s, err := NewServer(ServerConfig{
		PrivilegeNewestRegistration: true,
		AddTargetHandler:            func(Target) error { return nil },
		DeleteTargetHandler:         func(Target) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	for _, a := range []net.Addr{victim, winner} {
		if err := s.addClient(a, newConcurrentClientStream()); err != nil {
			t.Fatalf("addClient(%v): %v", a, err)
		}
	}

	victimClaimed := make(chan struct{})
	winnerEvicted := make(chan struct{})
	victimRecorded := make(chan struct{})

	go func() {
		// Victim claims the target in the ownership map (clean add)...
		evicted, err := s.addTargetToMap(victim, key)
		if err != nil || evicted != nil {
			t.Errorf("victim addTargetToMap: evicted=%v err=%v, want clean add", evicted, err)
		}
		close(victimClaimed)
		// ...is preempted here while the winner evicts it...
		<-winnerEvicted
		// ...then resumes and unconditionally records the target in its own set,
		// exactly as addTarget does at ~:520 with no ownership re-check.
		s.addTargetToClient(victim, key)
		close(victimRecorded)
	}()

	<-victimClaimed
	if err := s.addTarget(winner, tgt); err != nil {
		t.Fatalf("winner addTarget (evict): %v", err)
	}
	close(winnerEvicted)
	<-victimRecorded

	// #1/#3: rTargets says winner owns it, but both client sets now hold it.
	checkOwnership(t, s, []net.Addr{victim, winner}, key, "post-strand")

	// #2: the winner disconnects first, dropping rTargets[key]; the victim's
	// subsequent teardown then strands the target and deadlocks. Order matters:
	// while the winner still owns rTargets[key] the victim's delete takes the
	// harmless owned==false path instead.
	teardownWithWatchdog(t, s, []net.Addr{winner}, 2*time.Second, "winner teardown")
	teardownWithWatchdog(t, s, []net.Addr{victim}, 2*time.Second, "victim teardown")
}

// TestPrivilegeNewestRegistrationConcurrentStress drives the real addTarget and
// teardown paths from many clients racing on the SAME target, so it validates
// whatever fix lands (it goes through the public methods, not the internals).
// Run it under the race detector, ideally with -count and -cpu to widen coverage:
//
//	go test ./tunnel/ -run ConcurrentStress -race -count=20 -cpu=4
func TestPrivilegeNewestRegistrationConcurrentStress(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(4))
	}
	const (
		nClients    = 6
		nIterations = 500
	)
	key := Target{ID: "dev1", Type: "GNMI_GNOI"}
	tgt := &tpb.Target{Target: key.ID, TargetType: key.Type, Op: tpb.Target_ADD}

	for iter := 0; iter < nIterations; iter++ {
		s, err := NewServer(ServerConfig{
			PrivilegeNewestRegistration: true,
			AddTargetHandler:            func(Target) error { return nil },
			DeleteTargetHandler:         func(Target) error { return nil },
		})
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		addrs := make([]net.Addr, nClients)
		for i := range addrs {
			a, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", 45100+i))
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			addrs[i] = a
			if err := s.addClient(a, newConcurrentClientStream()); err != nil {
				t.Fatalf("addClient: %v", err)
			}
		}

		// Every client registers the same target as simultaneously as possible.
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, a := range addrs {
			wg.Add(1)
			go func(a net.Addr) {
				defer wg.Done()
				<-start
				_ = s.addTarget(a, tgt) // losers legitimately error; ignore
			}(a)
		}
		close(start)
		wg.Wait()

		// #1/#3: at quiescence the ownership map and client sets must agree. A
		// strand also guarantees a teardown deadlock, so stop here to keep the
		// failing run fast rather than waiting on the watchdog.
		if !checkOwnership(t, s, addrs, key, fmt.Sprintf("iter %d post-register", iter)) {
			return
		}

		// #2: with consistent state teardown is clean; the watchdog guards
		// against any interleaving that still wedges cmu.
		teardownWithWatchdog(t, s, addrs, 5*time.Second, fmt.Sprintf("iter %d teardown", iter))

		if got := s.clientTargets(nil); len(got) != 0 {
			t.Fatalf("iter %d: rTargets not empty after full teardown: %v", iter, got)
		}
	}
}
