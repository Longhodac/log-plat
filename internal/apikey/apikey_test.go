package apikey

import (
	"strings"
	"testing"
)

func TestParseAndLookup(t *testing.T) {
	s, err := Parse(" k1:hdfs , k2:apache,")
	if err != nil {
		t.Fatal(err)
	}
	if svc, ok := s.Service("k1"); !ok || svc != "hdfs" {
		t.Errorf("Service(k1) = %q, %v; want hdfs, true", svc, ok)
	}
	if _, ok := s.Service("k3"); ok {
		t.Error("unknown key resolved")
	}
}

func TestParseRejectsBadSpecsWithoutLeakingKeys(t *testing.T) {
	for _, spec := range []string{"", "secretkey", "secretkey:", ":svc", "a:x,a:y"} {
		_, err := Parse(spec)
		if err == nil {
			t.Errorf("Parse(%q) succeeded, want error", spec)
			continue
		}
		if strings.Contains(err.Error(), "secretkey") {
			t.Errorf("error for %q leaks the key: %v", spec, err)
		}
	}
}
