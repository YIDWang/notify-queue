package queue

import (
	"sync"
	"sync/atomic"
)

var chanPool = sync.Pool{
	New: func() interface{} {
		return make(chan interface{}, 2)
	},
}

func putChan(cc chan interface{}) {
	select {
	case <-cc:
	default:
	}
	chanPool.Put(cc)
}

type flagNode int

const (
	lisureNode flagNode = iota
	usedNode
	destroyNode
)

// 1 used
// 0 lisure
type Queue struct {
	qlock sync.RWMutex
	used  int32
	last  int // record the last position of the notification
	qchan []chan interface{}

	wg        sync.WaitGroup
	glock     sync.Mutex
	garbage   []int
	size      int
	gclock    sync.Mutex //Wait for manual execution to complete
	gcRunning int32

	valid      int32
	validCount int32
}

func NewQueue(sizeQueue, sizeGC int) *Queue {
	return &Queue{
		size:    sizeGC,
		qchan:   make([]chan interface{}, 0, sizeQueue),
		garbage: make([]int, 0, sizeGC),
	}
}

func (q *Queue) Destory() {
	if q.invalid() {
		return
	}

	// set queue invalid
	atomic.StoreInt32(&q.valid, 1)

	// wait gc goroutine
	q.wg.Wait()
	q.gclock.Lock()
	for _, c := range q.qchan {
		putChan(c)
	}
	q.gclock.Unlock()
}

func (q *Queue) GetNode() chan interface{} {
	if q.invalid() {
		return nil
	}

	if atomic.LoadInt32(&q.used) != 0 {
		return nil
	}
	rchan := chanPool.Get().(chan interface{})
	q.qlock.Lock()
	q.qchan = append(q.qchan, rchan)
	q.qlock.Unlock()

	atomic.AddInt32(&q.validCount, 1)
	return rchan
}

func (q *Queue) DiscardNode(schan chan interface{}) {
	if q.invalid() {
		return
	}
	schan <- destroyNode
	atomic.AddInt32(&q.validCount, -1)
}

func (q *Queue) ValidCount() int {
	return int(atomic.LoadInt32(&q.validCount))
}

func (q *Queue) Scan(message interface{}, handler func() bool) {
	if q.invalid() {
		return
	}

	q.qlock.RLock()
	atomic.AddInt32(&q.used, 1)
	defer q.qlock.RUnlock()
	defer atomic.AddInt32(&q.used, -1)

	indexs := make([]int, 0, 10)
	for i, rc := range q.qchan {
		if ok := q.updateChan(message, rc); !ok {
			q.glock.Lock()
			indexs = append(indexs, i)
			q.glock.Unlock()
			continue
		}
		if over := handler(); over {
			q.judgeGC(indexs)
			return
		}
	}
	q.judgeGC(indexs)
}

func (q *Queue) updateChan(message interface{}, cc chan interface{}) bool {
	select {
	case m := <-cc:
		if _, ok := m.(flagNode); ok {
			return false
		}
	default:
	}
	cc <- message
	return true
}

func (q *Queue) judgeGC(indexs []int) {
	q.glock.Lock()
	q.garbage = append(q.garbage, indexs...)
	if len(q.garbage) > q.size && atomic.CompareAndSwapInt32(&q.gcRunning, 0, 1) {
		q.wg.Add(1)
		go func(garbage []int) {
			q.gc(garbage)
			q.wg.Done()
		}(q.garbage)
		q.garbage = make([]int, 0, q.size)
	}
	q.glock.Unlock()
}

func (q *Queue) GC() bool {
	if q.invalid() {
		return false
	}

	q.glock.Lock()
	if !atomic.CompareAndSwapInt32(&q.gcRunning, 0, 1) {
		q.glock.Unlock()
		return false
	}
	garbage := q.garbage
	q.garbage = make([]int, 0, q.size)
	q.glock.Unlock()

	q.gc(garbage)
	return true
}

func (q *Queue) gc(garbage []int) {
	q.gclock.Lock()
	q.qlock.Lock()

	last := len(q.qchan) - 1
	for i, pos := range garbage {
		q.qchan[pos] = q.qchan[last-i]
	}
	q.qchan = q.qchan[:last-len(garbage)]

	q.qlock.Unlock()
	atomic.StoreInt32(&q.used, 0)

	q.gclock.Unlock()
	atomic.StoreInt32(&q.gcRunning, 0)
}

func (q *Queue) invalid() bool {
	if atomic.LoadInt32(&q.valid) == 1 {
		return true
	}
	return false
}
