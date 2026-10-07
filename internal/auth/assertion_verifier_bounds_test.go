package auth

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The controlled HTTPS issuer counts each request before its response. These
// tests join callers before inspecting counts; client activity is not the oracle.
func boundsDirectRule(issuer *controlledIssuer) FederationRule {
	rule := issuer.rule
	rule.DiscoveryURL = ""
	rule.JWKSURL = issuer.server.URL + "/keys"
	return rule
}

func boundsVerifier(t *testing.T) (*AssertionVerifier, *atomic.Int64) {
	t.Helper()
	clock := &atomic.Int64{}
	clock.Store(time.Now().UnixNano())
	verifier := NewAssertionVerifier(context.Background())
	verifier.now = func() time.Time { return time.Unix(0, clock.Load()) }
	t.Cleanup(verifier.Close)
	return verifier, clock
}

func boundsBlockIssuer(t *testing.T, issuer *controlledIssuer, requests int) (<-chan struct{}, func()) {
	t.Helper()
	block := make(chan struct{})
	entered := make(chan struct{}, requests)
	issuer.mu.Lock()
	issuer.block, issuer.entered = block, entered
	issuer.mu.Unlock()
	var once sync.Once
	release := func() { once.Do(func() { close(block) }) }
	t.Cleanup(release)
	return entered, release
}

func boundsWaitEntered(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("controlled HTTPS request did not enter its server barrier")
	}
}

func boundsWaitResult(t *testing.T, results <-chan error) error {
	t.Helper()
	select {
	case err := <-results:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("verifier caller did not join within its bounded request lifetime")
		return nil
	}
}

func boundsVerifyBurst(ctx context.Context, t *testing.T, verifier *AssertionVerifier, rule FederationRule, assertion string, callers int) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); workers.Wait() })
	results := make(chan error, callers)
	start := make(chan struct{})
	for range callers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, err := verifier.Verify(ctx, rule, assertion)
			results <- err
		}()
	}
	close(start)
	return results
}

func boundsAssertUnavailable(t *testing.T, err error) {
	t.Helper()
	var unavailable *UnavailableError
	var invalid *AuthenticationError
	if !errors.As(err, &unavailable) || errors.As(err, &invalid) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected dependency unavailable without caller cancellation, got %T: %v", err, err)
	}
}

func TestAssertionVerifierConcurrentRefreshSingleflight(t *testing.T) {
	for _, warm := range []bool{false, true} {
		name := "cold known key"
		if warm {
			name = "warm valid new kid"
		}
		t.Run(name, func(t *testing.T) {
			issuer := newControlledIssuer(t)
			verifier, _ := boundsVerifier(t)
			rule := boundsDirectRule(issuer)
			kid := "initial"
			if warm {
				assertion := signIssuerAssertion(t, issuer.key, kid, rule, nil)
				if _, err := verifier.Verify(context.Background(), rule, assertion); err != nil {
					t.Fatal(err)
				}
				kid = "new-key-id"
				issuer.mu.Lock()
				issuer.keys[kid] = issuer.key
				issuer.mu.Unlock()
			}
			// Signing helpers can fail the test, so they run outside workers.
			assertion := signIssuerAssertion(t, issuer.key, kid, rule, nil)
			entered, release := boundsBlockIssuer(t, issuer, 32)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			before := issuer.count("/keys")
			const callers = 32
			results := boundsVerifyBurst(ctx, t, verifier, rule, assertion, callers)
			boundsWaitEntered(t, entered)
			if got := issuer.count("/keys"); got != before+1 {
				t.Fatalf("pending refresh reached issuer %d times, want %d", got, before+1)
			}
			// Keep the leader blocked while a second valid caller exhausts its
			// own deadline. A cooldown-before-join bug returns invalid early.
			waitCtx, waitCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err := verifier.Verify(waitCtx, rule, assertion)
			waitCancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("blocked valid waiter did not retain its own deadline: %T: %v", err, err)
			}
			if got := issuer.count("/keys"); got != before+1 {
				t.Fatal("deadline-limited waiter created a second issuer refresh")
			}
			release()
			for range callers {
				if err := boundsWaitResult(t, results); err != nil {
					t.Fatalf("joined valid assertion was rejected: %v", err)
				}
			}
			if got := issuer.count("/keys"); got != before+1 {
				t.Fatalf("concurrent assertion burst fetched %d times, want one additional request", got-before)
			}
		})
	}
}

func TestAssertionVerifierRevisionRefreshIsolation(t *testing.T) {
	oldIssuer := newControlledIssuer(t)
	newIssuer := newControlledIssuer(t)
	verifier, _ := boundsVerifier(t)
	oldRule := boundsDirectRule(oldIssuer)
	newRule := boundsDirectRule(newIssuer)
	newRule.ID, newRule.Revision = oldRule.ID, oldRule.Revision+1
	oldAssertion := signIssuerAssertion(t, oldIssuer.key, "initial", oldRule, nil)
	newAssertion := signIssuerAssertion(t, newIssuer.key, "initial", newRule, nil)
	entered, release := boundsBlockIssuer(t, oldIssuer, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	oldResult := boundsVerifyBurst(ctx, t, verifier, oldRule, oldAssertion, 1)
	boundsWaitEntered(t, entered)
	proof, err := verifier.Verify(ctx, newRule, newAssertion)
	if err != nil || proof.RuleRevision() != newRule.Revision || proof.Issuer() != newRule.Issuer {
		t.Fatalf("new revision failed before old refresh completed: %v", err)
	}
	release()
	assertInvalidAssertion(t, boundsWaitResult(t, oldResult))
	proof, err = verifier.Verify(ctx, newRule, newAssertion)
	if err != nil || proof.RuleRevision() != newRule.Revision || proof.Issuer() != newRule.Issuer {
		t.Fatalf("late old-revision refresh corrupted current trusted proof: %v", err)
	}
	_, err = verifier.Verify(ctx, oldRule, oldAssertion)
	assertInvalidAssertion(t, err)
	if oldIssuer.count("/keys") != 1 || newIssuer.count("/keys") != 1 {
		t.Fatal("revision isolation reused old keys or refetched the current trusted revision")
	}
}

func TestAssertionVerifierRefreshAndCacheCapacity(t *testing.T) {
	issuer := newControlledIssuer(t)
	verifier, _ := boundsVerifier(t)
	rule := boundsDirectRule(issuer)
	assertion := signIssuerAssertion(t, issuer.key, "initial", rule, nil)
	// More independent registrations than cache capacity must remain serviceable
	// through eviction while retaining the documented memory bound.
	const warmed = 130
	for n := range warmed {
		rule.ID = fmt.Sprintf("capacity-warm-%d", n)
		if _, err := verifier.Verify(context.Background(), rule, assertion); err != nil {
			t.Fatalf("cache pressure rejected registration %d: %v", n, err)
		}
		verifier.mu.Lock()
		cacheSize, revisionSize := len(verifier.cache), len(verifier.current)
		verifier.mu.Unlock()
		if cacheSize > 128 || revisionSize > 128 {
			t.Fatalf("cache/revision storage exceeded 128 entries: %d/%d", cacheSize, revisionSize)
		}
	}
	if issuer.count("/keys") != warmed {
		t.Fatal("cold independent registrations did not reach actual HTTPS issuer once each")
	}
	verifier.mu.Lock()
	evicted := ""
	for n := range warmed {
		candidate := fmt.Sprintf("capacity-warm-%d", n)
		if verifier.cache[issuerRevision{ID: candidate, Revision: rule.Revision}] == nil {
			evicted = candidate
			break
		}
	}
	verifier.mu.Unlock()
	if evicted == "" {
		t.Fatal("cache pressure retained every registration beyond its capacity")
	}
	rule.ID = evicted
	if _, err := verifier.Verify(context.Background(), rule, assertion); err != nil || issuer.count("/keys") != warmed+1 {
		t.Fatalf("evicted registration did not refetch and recover: %v", err)
	}

	entered, release := boundsBlockIssuer(t, issuer, 32)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	results := make(chan error, 16)
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); workers.Wait() })
	for n := range 16 {
		refreshRule := rule
		refreshRule.ID = fmt.Sprintf("capacity-refresh-%d", n)
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := verifier.Verify(ctx, refreshRule, assertion)
			results <- err
		}()
	}
	for range 16 {
		boundsWaitEntered(t, entered)
	}
	before := issuer.count("/keys")
	if before != warmed+1+16 {
		t.Fatalf("sixteen active refreshes reached issuer %d times, want %d", before, warmed+1+16)
	}
	verifier.mu.Lock()
	cacheSize, revisionSize, active := len(verifier.cache), len(verifier.current), verifier.refreshes
	waitRule := rule
	waitRule.ID = ""
	for revision, entry := range verifier.cache {
		if entry.refreshing != nil && verifier.current[revision.ID] == revision.Revision {
			waitRule.ID = revision.ID
			break
		}
	}
	verifier.mu.Unlock()
	if cacheSize > 128 || revisionSize > 128 || active != 16 {
		t.Fatalf("pressure exceeded capacity or lost active refresh accounting: cache=%d revisions=%d refreshes=%d", cacheSize, revisionSize, active)
	}
	// A waiting caller owns its own cancellation and cannot cancel the leader.
	if waitRule.ID == "" {
		t.Fatal("cache pressure lost every current active registration")
	}
	waitCtx, waitCancel := context.WithCancel(context.Background())
	waitCancel()
	_, err := verifier.Verify(waitCtx, waitRule, assertion)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting caller lost its own cancellation: %T: %v", err, err)
	}
	if issuer.count("/keys") != before {
		t.Fatal("canceled waiting caller created a second issuer request")
	}
	limited := rule
	limited.ID = "capacity-refused"
	_, err = verifier.Verify(context.Background(), limited, assertion)
	boundsAssertUnavailable(t, err)
	if issuer.count("/keys") != before {
		t.Fatal("seventeenth simultaneous refresh bypassed its admission bound")
	}
	cancel()
	for range 16 {
		if err := boundsWaitResult(t, results); !errors.Is(err, context.Canceled) {
			t.Fatalf("active refresh cancellation misclassified: %T: %v", err, err)
		}
	}
	release()
	verifier.mu.Lock()
	active = verifier.refreshes
	verifier.mu.Unlock()
	if active != 0 {
		t.Fatal("joined cancellations retained occupied refresh slots")
	}
	issuer.mu.Lock()
	issuer.block, issuer.entered = nil, nil
	issuer.mu.Unlock()
	if _, err := verifier.Verify(context.Background(), limited, assertion); err != nil || issuer.count("/keys") != before+1 {
		t.Fatalf("released refresh slot did not admit and verify the refused registration: %v", err)
	}
}

func TestAssertionVerifierActiveRefreshSurvivesCachePressure(t *testing.T) {
	issuer := newControlledIssuer(t)
	verifier, _ := boundsVerifier(t)
	rule := boundsDirectRule(issuer)
	assertion := signIssuerAssertion(t, issuer.key, "initial", rule, nil)
	entered, release := boundsBlockIssuer(t, issuer, 16)
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); workers.Wait() })
	results := make(chan error, 16)
	for n := range 16 {
		activeRule := rule
		activeRule.ID = fmt.Sprintf("pressure-active-%d", n)
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := verifier.Verify(ctx, activeRule, assertion)
			results <- err
		}()
	}
	for range 16 {
		boundsWaitEntered(t, entered)
	}
	// Requests refused by the refresh bound still exercise registration/cache
	// capacity. They must not erase a valid active revision's publication right.
	for n := range 256 {
		refused := rule
		refused.ID = fmt.Sprintf("pressure-refused-%d", n)
		_, err := verifier.Verify(ctx, refused, assertion)
		boundsAssertUnavailable(t, err)
	}
	if issuer.count("/keys") != 16 {
		t.Fatal("capacity-refused registrations bypassed the sixteen-refresh bound")
	}
	verifier.mu.Lock()
	cacheSize, revisionSize := len(verifier.cache), len(verifier.current)
	verifier.mu.Unlock()
	if cacheSize > 128 || revisionSize > 128 {
		t.Fatalf("active refresh protection removed storage bounds: cache=%d revisions=%d", cacheSize, revisionSize)
	}
	release()
	for range 16 {
		if err := boundsWaitResult(t, results); err != nil {
			t.Fatalf("valid active refresh lost its current revision under cache pressure: %v", err)
		}
	}
	// Every admitted registration must now verify from its own completed cache.
	for n := range 16 {
		activeRule := rule
		activeRule.ID = fmt.Sprintf("pressure-active-%d", n)
		proof, err := verifier.Verify(ctx, activeRule, assertion)
		if err != nil || proof.RuleID() != activeRule.ID {
			t.Fatalf("completed active registration lost its cached proof: %v", err)
		}
	}
	if issuer.count("/keys") != 16 {
		t.Fatal("surviving active registrations had to refetch after pressure")
	}
}

func TestAssertionVerifierFailedRefreshCooldown(t *testing.T) {
	for _, expired := range []bool{false, true} {
		for _, unavailable := range []bool{false, true} {
			name := fmt.Sprintf("expired=%t/unavailable=%t", expired, unavailable)
			t.Run(name, func(t *testing.T) {
				issuer := newControlledIssuer(t)
				verifier, clock := boundsVerifier(t)
				rule := boundsDirectRule(issuer)
				assertion := signIssuerAssertion(t, issuer.key, "initial", rule, nil)
				if expired {
					if _, err := verifier.Verify(context.Background(), rule, assertion); err != nil {
						t.Fatal(err)
					}
					clock.Add(int64(issuerKeyTTL))
				}
				issuer.mu.Lock()
				if unavailable {
					issuer.status = http.StatusServiceUnavailable
				} else {
					issuer.keys = nil
				}
				issuer.mu.Unlock()
				check := func(err error) {
					t.Helper()
					if unavailable {
						boundsAssertUnavailable(t, err)
					} else {
						assertInvalidAssertion(t, err)
					}
				}
				before := issuer.count("/keys")
				_, err := verifier.Verify(context.Background(), rule, assertion)
				check(err)
				for range 2 {
					results := boundsVerifyBurst(context.Background(), t, verifier, rule, assertion, 32)
					for range 32 {
						check(boundsWaitResult(t, results))
					}
					if issuer.count("/keys") != before+1 {
						t.Fatal("failed cold/expired burst bypassed the request-attempt cooldown")
					}
					clock.Add(int64(issuerUnknownKidCooldown - time.Nanosecond))
					_, err := verifier.Verify(context.Background(), rule, assertion)
					check(err)
					if issuer.count("/keys") != before+1 {
						t.Fatal("cooldown was shortened before its recorded equality boundary")
					}
					clock.Add(int64(time.Nanosecond))
					before++
					_, err = verifier.Verify(context.Background(), rule, assertion)
					check(err)
					if issuer.count("/keys") != before+1 {
						t.Fatal("cooldown equality did not permit exactly one new request attempt")
					}
				}
			})
		}
	}
}

// joinSignalContext reports its first Done call. Verify reads Done only when a
// caller waits on a joined refresh or derives its own refresh context, so while
// the initiating caller is held at the issuer barrier the signal proves a join.
type joinSignalContext struct {
	context.Context
	once   sync.Once
	joined chan struct{}
}

func (c *joinSignalContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.joined) })
	return c.Context.Done()
}

// boundsJoinLiveCaller starts a live caller and returns once it has joined the
// refresh currently held at the issuer barrier.
func boundsJoinLiveCaller(t *testing.T, verifier *AssertionVerifier, rule FederationRule, assertion string) <-chan error {
	t.Helper()
	base, cancel := context.WithCancel(context.Background())
	ctx := &joinSignalContext{Context: base, joined: make(chan struct{})}
	result := make(chan error, 1)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		_, err := verifier.Verify(ctx, rule, assertion)
		result <- err
	}()
	t.Cleanup(func() { cancel(); <-exited })
	select {
	case <-ctx.joined:
	case <-time.After(2 * time.Second):
		t.Fatal("live caller did not join the in-flight refresh")
	}
	return result
}

// A refresh abandoned because its initiating caller's own context ended is not
// issuer evidence. It records no failure, cooldown or forced-refresh use, and it
// never discards keys that already loaded. Live failures keep their cooldown in
// TestAssertionVerifierFailedRefreshCooldown.
func TestAssertionVerifierCallerCancellationDoesNotRecordIssuerFailure(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("joined caller retries/deadline=%t", deadline), func(t *testing.T) {
			issuer := newControlledIssuer(t)
			verifier, _ := boundsVerifier(t)
			rule := boundsDirectRule(issuer)
			assertion := signIssuerAssertion(t, issuer.key, "initial", rule, nil)
			entered, release := boundsBlockIssuer(t, issuer, 2)
			ctx, cancel := context.WithCancel(context.Background())
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			}
			t.Cleanup(cancel)
			initiator := boundsVerifyBurst(ctx, t, verifier, rule, assertion, 1)
			boundsWaitEntered(t, entered)
			live := boundsJoinLiveCaller(t, verifier, rule, assertion)
			if ctx.Err() != nil {
				t.Fatal("initiating caller ended before the live caller joined its refresh")
			}
			if deadline {
				<-ctx.Done()
			} else {
				cancel()
			}
			if err := boundsWaitResult(t, initiator); !errors.Is(err, ctx.Err()) {
				t.Fatalf("initiating caller lost its own context error: %T: %v", err, err)
			}
			// The joined live caller leads a new bounded refresh of its own.
			boundsWaitEntered(t, entered)
			release()
			if err := boundsWaitResult(t, live); err != nil {
				t.Fatalf("joined live caller inherited the abandoned refresh: %T: %v", err, err)
			}
			if got := issuer.count("/keys"); got != 2 {
				t.Fatalf("abandoned refresh and its live retry reached the issuer %d times, want 2", got)
			}
			if _, err := verifier.Verify(context.Background(), rule, assertion); err != nil || issuer.count("/keys") != 2 {
				t.Fatalf("later live caller did not verify from the published keys: %v", err)
			}
		})
	}
	// An abandoned forced refresh does not consume the unknown-kid forced
	// refresh. The verifier clock stays inside the 30-second window that a
	// recorded forced refresh would close for this kid.
	t.Run("abandoned forced refresh keeps unknown-kid refresh", func(t *testing.T) {
		issuer := newControlledIssuer(t)
		verifier, _ := boundsVerifier(t)
		rule := boundsDirectRule(issuer)
		if _, err := verifier.Verify(context.Background(), rule, signIssuerAssertion(t, issuer.key, "initial", rule, nil)); err != nil {
			t.Fatal(err)
		}
		issuer.mu.Lock()
		issuer.keys["new-key-id"] = issuer.key
		issuer.mu.Unlock()
		assertion := signIssuerAssertion(t, issuer.key, "new-key-id", rule, nil)
		entered, release := boundsBlockIssuer(t, issuer, 1)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		initiator := boundsVerifyBurst(ctx, t, verifier, rule, assertion, 1)
		boundsWaitEntered(t, entered)
		cancel()
		if err := boundsWaitResult(t, initiator); !errors.Is(err, context.Canceled) {
			t.Fatalf("initiating caller lost its own cancellation: %T: %v", err, err)
		}
		release()
		if _, err := verifier.Verify(context.Background(), rule, assertion); err != nil {
			t.Fatalf("abandoned forced refresh consumed the unknown-kid refresh: %T: %v", err, err)
		}
		if got := issuer.count("/keys"); got != 3 {
			t.Fatalf("live new-kid caller made %d forced requests, want exactly one", got-2)
		}
	})
	t.Run("caller ends after keys load", func(t *testing.T) {
		issuer := newControlledIssuer(t)
		verifier, _ := boundsVerifier(t)
		rule := boundsDirectRule(issuer)
		assertion := signIssuerAssertion(t, issuer.key, "initial", rule, nil)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		load := verifier.loadKeys
		verifier.loadKeys = func(loadCtx context.Context, rule FederationRule) (map[string]*rsa.PublicKey, error) {
			keys, err := load(loadCtx, rule)
			cancel()
			return keys, err
		}
		if _, err := verifier.Verify(ctx, rule, assertion); !errors.Is(err, context.Canceled) {
			t.Fatalf("initiating caller lost its own cancellation: %T: %v", err, err)
		}
		if _, err := verifier.Verify(context.Background(), rule, assertion); err != nil || issuer.count("/keys") != 1 {
			t.Fatalf("keys loaded before the initiating caller ended were discarded: %v", err)
		}
	})
}

func TestAssertionVerifierRecordedValidityOutageBoundary(t *testing.T) {
	for _, ttl := range []time.Duration{0, time.Second - time.Nanosecond, 10*time.Minute + time.Nanosecond} {
		verifier, err := NewConfiguredAssertionVerifier(context.Background(), AssertionVerifierConfig{KeyCacheTTL: ttl})
		var invalid *ValidationError
		if verifier != nil {
			verifier.Close()
		}
		if verifier != nil || !errors.As(err, &invalid) {
			t.Fatalf("out-of-bound TTL %s did not fail typed construction: %v", ttl, err)
		}
	}
	for _, configured := range []time.Duration{0, time.Second, time.Minute, 10 * time.Minute} {
		name := "default"
		if configured != 0 {
			name = configured.String()
		}
		t.Run(name, func(t *testing.T) {
			issuer := newControlledIssuer(t)
			var verifier *AssertionVerifier
			ttl := configured
			if configured == 0 {
				verifier = NewAssertionVerifier(context.Background())
				ttl = 10 * time.Minute
			} else {
				var err error
				verifier, err = NewConfiguredAssertionVerifier(context.Background(), AssertionVerifierConfig{KeyCacheTTL: configured})
				if err != nil {
					t.Fatal(err)
				}
			}
			clock := &atomic.Int64{}
			clock.Store(time.Now().UnixNano())
			verifier.now = func() time.Time { return time.Unix(0, clock.Load()) }
			t.Cleanup(verifier.Close)
			rule := boundsDirectRule(issuer)
			assertion := signIssuerAssertion(t, issuer.key, "initial", rule, nil)
			if _, err := verifier.Verify(context.Background(), rule, assertion); err != nil {
				t.Fatal(err)
			}
			issuer.mu.Lock()
			issuer.status = http.StatusServiceUnavailable
			issuer.mu.Unlock()
			clock.Add(int64(ttl - time.Nanosecond))
			if _, err := verifier.Verify(context.Background(), rule, assertion); err != nil {
				t.Fatalf("known key was not served immediately before its recorded expiry: %v", err)
			}
			if issuer.count("/keys") != 2 {
				t.Fatal("near-expiry validity control did not actually attempt failed HTTPS refresh")
			}
			clock.Add(int64(time.Nanosecond))
			for range 4 {
				_, err := verifier.Verify(context.Background(), rule, assertion)
				boundsAssertUnavailable(t, err)
			}
			if issuer.count("/keys") != 2 {
				t.Fatal("TTL equality either extended recorded validity or bypassed failure cooldown")
			}
		})
	}
}

func TestAssertionVerifierSameRevisionKeyRetirement(t *testing.T) {
	issuer := newControlledIssuer(t)
	newSigner := newControlledIssuer(t)
	verifier, clock := boundsVerifier(t)
	rule := boundsDirectRule(issuer)
	old := signIssuerAssertion(t, issuer.key, "initial", rule, nil)
	fresh := signIssuerAssertion(t, newSigner.key, "new-key-id", rule, nil)
	if _, err := verifier.Verify(context.Background(), rule, old); err != nil {
		t.Fatal(err)
	}
	issuer.mu.Lock()
	issuer.keys["new-key-id"] = newSigner.key
	issuer.mu.Unlock()
	if _, err := verifier.Verify(context.Background(), rule, fresh); err != nil {
		t.Fatal("overlap refresh did not accept new signing key", err)
	}
	issuer.mu.Lock()
	delete(issuer.keys, "initial")
	issuer.mu.Unlock()
	clock.Add(int64(issuerKeyTTL - time.Minute - time.Nanosecond))
	if _, err := verifier.Verify(context.Background(), rule, old); err != nil || issuer.count("/keys") != 2 {
		t.Fatalf("old key's recorded cached validity was lost before a new refresh: %v", err)
	}
	clock.Add(int64(time.Minute + time.Nanosecond))
	_, err := verifier.Verify(context.Background(), rule, old)
	assertInvalidAssertion(t, err)
	if issuer.count("/keys") != 3 {
		t.Fatal("same-revision retirement did not fetch the actual changed JWKS at recorded TTL expiry")
	}
	if _, err := verifier.Verify(context.Background(), rule, fresh); err != nil {
		t.Fatal("same-revision retirement removed the current signer", err)
	}
	if issuer.count("/keys") != 3 {
		t.Fatal("known current signer triggered an unnecessary refresh after retirement")
	}
	// The prior request refreshed a previously known key at normal TTL expiry.
	// Its now-unknown kid may force one refresh; repeats must honor cooldown.
	_, err = verifier.Verify(context.Background(), rule, old)
	assertInvalidAssertion(t, err)
	afterForced := issuer.count("/keys")
	if afterForced < 3 || afterForced > 4 {
		t.Fatal("unknown retired kid consumed more than one forced refresh after normal TTL refresh")
	}
	for range 4 {
		_, err = verifier.Verify(context.Background(), rule, old)
		assertInvalidAssertion(t, err)
	}
	if issuer.count("/keys") != afterForced {
		t.Fatal("repeated retired kid bypassed unknown-kid cooldown")
	}
}
