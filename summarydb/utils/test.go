package utils

import "testing"

func AssertTrue(t *testing.T, a bool) {
	if !a {
		t.Fatalf("Expected true, got false")
	}
}

func AssertEqual(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Fatalf("Expected equal: %s != %s\n", a, b)
	}
}
