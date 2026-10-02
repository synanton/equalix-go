package cms

import "testing"

func BenchmarkAdd(b *testing.B) {
	s := New(65536, 5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Add("tenant-123", 1)
	}
}

func BenchmarkEstimateCount(b *testing.B) {
	s := New(65536, 5)
	s.Add("tenant-123", 7)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.EstimateCount("tenant-123")
	}
}
