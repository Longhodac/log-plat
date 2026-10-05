// Package apikey maps API keys to the service identity they authenticate.
package apikey

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
)

// Store resolves keys by their SHA-256 digest, so lookups do not leak key
// prefixes through timing and the plaintext keys are not retained.
type Store struct {
	byDigest map[[sha256.Size]byte]string
}

// Parse reads "key1:service-a,key2:service-b".
func Parse(spec string) (*Store, error) {
	s := &Store{byDigest: map[[sha256.Size]byte]string{}}
	for _, pair := range strings.Split(spec, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, service, ok := strings.Cut(pair, ":")
		key, service = strings.TrimSpace(key), strings.TrimSpace(service)
		if !ok || key == "" || service == "" {
			return nil, fmt.Errorf("api key entry %q: want key:service", redact(pair))
		}
		d := sha256.Sum256([]byte(key))
		if _, dup := s.byDigest[d]; dup {
			return nil, fmt.Errorf("api key for service %q is listed twice", service)
		}
		s.byDigest[d] = service
	}
	if len(s.byDigest) == 0 {
		return nil, errors.New("no api keys configured")
	}
	return s, nil
}

// Service returns the service a key belongs to.
func (s *Store) Service(key string) (string, bool) {
	svc, ok := s.byDigest[sha256.Sum256([]byte(key))]
	return svc, ok
}

func redact(pair string) string {
	_, service, _ := strings.Cut(pair, ":")
	return "***:" + service
}
