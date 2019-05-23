package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	q := NewQueue(10, 1)
	nc := make([]chan interface{}, 0)
	for i := 0; i < 10; i++ {
		nc = append(nc, q.GetNode())
	}
	assert.Equal(t, 10, q.ValidCount())
	q.DiscardNode(nc[0])
	q.DiscardNode(nc[1])
	assert.Equal(t, 8, q.ValidCount())
	count := 0
	q.Scan(1, func() bool {
		count++
		return false
	})
	assert.Equal(t, count, q.ValidCount())
	q.Destory()
}

func BenchmarkQueueGet(b *testing.B) {
	q := NewQueue(b.N, 1)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.GetNode()
	}
}

func BenchmarkQueueDiscard(b *testing.B) {
	q := NewQueue(b.N, 1)
	nc := make([]chan interface{}, 0, b.N)
	for i := 0; i < b.N; i++ {
		nc = append(nc, q.GetNode())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.DiscardNode(nc[i])
	}
}

func BenchmarkBigSetScan(b *testing.B) {
	clientNum := 10000
	q := NewQueue(clientNum, 1)
	nc := make([]chan interface{}, 0, clientNum)
	for i := 0; i < clientNum; i++ {
		nc = append(nc, q.GetNode())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Scan(i, func() bool { return false })
	}
}
