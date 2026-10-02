package domain

import (
	"testing"
	"time"
)

func BenchmarkReserve(b *testing.B) {
	s := NewStore()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Reserve("tenant-123", DefaultQuantum, 1.0)
	}
}

func BenchmarkCalculatePriority(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CalculatePriority(42000, 7, 40, 2.0)
	}
}

func BenchmarkSelectBatch(b *testing.B) {
	tasks := make([]*Task, 200)
	for i := range tasks {
		tasks[i] = &Task{ID: string(rune('a'+i%26)) + string(rune('0'+i%10)),
			FairnessKey: "k", Status: StatusQueued, Priority: int64(i), HasPriority: true}
	}
	zero := func(string) int { return 0 }
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SelectBatch(tasks, 10, 0, zero)
	}
}

func BenchmarkRankByAging(b *testing.B) {
	now := time.Now()
	tasks := make([]*Task, 400) // candidate pool: best-200 + oldest-200
	for i := range tasks {
		tasks[i] = &Task{ID: string(rune('a'+i%26)) + string(rune('0'+i%10)),
			FairnessKey: "k", Status: StatusQueued, Priority: int64(i * 100),
			HasPriority: true, CreatedAt: now.Add(-time.Duration(i) * time.Second)}
	}
	zero := func(string) int { return 0 }
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RankByAging(tasks, 10, AgingLinear, 1000, 0, now, 0, zero)
	}
}
