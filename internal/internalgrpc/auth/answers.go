package auth

import (
	"container/list"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"sync"
	"time"
)

const (
	// answerTTL is how long a completed review's answer, accepted or
	// rejected, is reused: 10 s, the cache time of Kubernetes' delegating
	// authenticator.
	answerTTL = 10 * time.Second
	// maxAnswers bounds the memory that answers hold.
	maxAnswers = 4096
)

// answer is the outcome of one completed TokenReview.
type answer struct {
	identity Identity
	err      error
}

// answers holds recent answers by an HMAC-SHA256 of their token under a
// random key of its own, so it holds no token. Every entry has the same
// lifetime, so the oldest entry is always the first to expire.
type answers struct {
	secret  [32]byte
	mu      sync.Mutex
	entries map[[sha256.Size]byte]*list.Element
	order   *list.List
}

type storedAnswer struct {
	key     [sha256.Size]byte
	expires time.Time
	answer  answer
}

func newAnswers() *answers {
	a := &answers{entries: map[[sha256.Size]byte]*list.Element{}, order: list.New()}
	_, _ = rand.Read(a.secret[:])
	return a
}

func (a *answers) key(token string) [sha256.Size]byte {
	mac := hmac.New(sha256.New, a.secret[:])
	mac.Write([]byte(token))
	var key [sha256.Size]byte
	copy(key[:], mac.Sum(nil))
	return key
}

func (a *answers) get(key [sha256.Size]byte, now time.Time) (answer, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	element, ok := a.entries[key]
	if !ok {
		return answer{}, false
	}
	stored := element.Value.(*storedAnswer)
	if !now.Before(stored.expires) {
		a.order.Remove(element)
		delete(a.entries, key)
		return answer{}, false
	}
	return stored.answer, true
}

func (a *answers) put(key [sha256.Size]byte, value answer, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if element, ok := a.entries[key]; ok {
		a.order.Remove(element)
	}
	a.entries[key] = a.order.PushBack(&storedAnswer{key: key, expires: now.Add(answerTTL), answer: value})
	for a.order.Len() > maxAnswers {
		oldest := a.order.Front()
		a.order.Remove(oldest)
		delete(a.entries, oldest.Value.(*storedAnswer).key)
	}
}
