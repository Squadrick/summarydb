package utils

import (
	"math"
	"testing"
)

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

func AssertClose(t *testing.T, a float64, b float64, eps float64) {
	diff := math.Abs(a - b)

	if diff > eps {
		t.Fatalf("Expected close: | %f - %f | > %f", a, b, eps)
	}
}
