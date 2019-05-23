package queue

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"
)

var (
	ERROR_NODE_EXISTED = errors.New("node has been existed")
	ERROR_NODE_DELETED = errors.New("node has been delete")
)

// item pool item循环使用
var itemPool *sync.Pool = &sync.Pool{
	New: func() interface{} { return NewItem() },
}

func GetItem(c interface{}) *Item {
	item := itemPool.Get().(*Item)
	item.c = unsafe.Pointer(&c)
	item.Prev = nil
	item.Next = nil
	return item
}

func PutItem(item *Item) {
	itemPool.Put(item)
}

type Item struct {
	c    unsafe.Pointer
	Prev *Item
	Next *Item
}

func NewItem() *Item {
	it := &Item{
		c:    nil,
		Prev: nil,
		Next: nil,
	}
	return it
}

// syncmap 封装线程安全的map
type syncMap struct {
	mapItem map[interface{}]*Item
	mapLock sync.RWMutex
}

func NewSyncMap() *syncMap {
	return &syncMap{
		mapItem: make(map[interface{}]*Item),
	}
}

//addMap 线程安全的元素添加
func (m *syncMap) addMap(c interface{}, item *Item) bool {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()
	if _, exist := m.mapItem[c]; !exist {
		m.mapItem[c] = item
		return true
	}
	return false
}

//judge 线程安全的判断map 中是否存在这个元素
func (m *syncMap) judge(c interface{}) *Item {
	m.mapLock.RLock()
	defer m.mapLock.RUnlock()
	if item, exist := m.mapItem[c]; exist {
		return item
	}
	return nil
}

//delMap 线程安全的元素删除
func (m *syncMap) delMap(c interface{}) *Item {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()
	if item, exist := m.mapItem[c]; exist {
		delete(m.mapItem, c)
		return item
	}
	return nil
}

//sizeMap 线程安全 返回map长度
func (m *syncMap) sizeMap() int {
	m.mapLock.RLock()
	defer m.mapLock.RUnlock()
	return len(m.mapItem)
}

//BigSet item的集合 提供并发安全的添加，删除和遍历
//使用场景要求要有高频遍历
type BigSet struct {
	head     *Item
	tail     *Item // TODO 暂时未使用到
	listLock sync.RWMutex

	mapItem  *syncMap
	delLock  sync.Mutex
	delQueue []*Item

	discardThreshold int
	clearLock        sync.RWMutex
	wg               sync.WaitGroup
}

func NewBigSet(discardThreshold int) *BigSet {
	bs := &BigSet{
		discardThreshold: discardThreshold,
		mapItem:          NewSyncMap(),
		delQueue:         make([]*Item, 0, discardThreshold),
	}
	return bs
}

func (bs *BigSet) delList(item *Item) {
	if item.Prev != nil {
		item.Prev.Next = item.Next
	} else {
		bs.head = item.Next
	}
	if item.Next != nil {
		item.Next.Prev = item.Prev
	} else {
		bs.tail = item.Prev
	}
}

func (bs *BigSet) addList(item *Item) {
	if bs.head == nil {
		bs.tail = item
		bs.head = item
		return
	}
	// 将新元素插入到结尾
	item.Prev = bs.tail
	bs.tail.Next = item
	bs.tail = item
}

//Add 元素的添加，map和list 两个数据结构通知添加
func (bs *BigSet) Put(c interface{}) error {
	item := GetItem(c)
	if bs.mapItem.addMap(c, item) {
		bs.listLock.Lock()
		bs.addList(item)
		bs.listLock.Unlock()
		return nil
	}
	PutItem(item)
	return ERROR_NODE_EXISTED
}

//Delete  元素的删除 在mapItem 删除和delQueue 添加
func (bs *BigSet) Delete(c interface{}) error {
	if it := bs.mapItem.delMap(c); it != nil {
		atomic.StorePointer(&it.c, nil)
		bs.delLock.Lock()
		bs.delQueue = append(bs.delQueue, it)
		if len(bs.delQueue) >= bs.discardThreshold {
			queue := bs.delQueue
			bs.delQueue = make([]*Item, 0, bs.discardThreshold)
			bs.wg.Add(1)
			go bs.clear(queue)
		}
		bs.delLock.Unlock()
		return nil
	}
	return ERROR_NODE_DELETED
}

//Scan 遍历函数 handlers  回调函数
func (bs *BigSet) Scan(handlers func(c interface{}) error) {
	bs.clearLock.RLock()
	defer bs.clearLock.RUnlock()

	bs.listLock.RLock()
	item := bs.head
	end := bs.tail
	bs.listLock.RUnlock()

	// 由于采用尾插法进行插入元素，所以这里只遍历到开始时候的结尾
	for item != end {
		c := (atomic.LoadPointer(&item.c))
		if c != nil {
			conn := *(*interface{})(c)
			if err := handlers(conn); err != nil {
				return
			}
		}
		item = item.Next
	}
	// 遍历最后一个元素
	if end != nil {
		c := (atomic.LoadPointer(&end.c))
		if c != nil {
			conn := *(*interface{})(c)
			if err := handlers(conn); err != nil {
				return
			}
		}
	}
}

//TotalSize 当前元素的总数数量 : 有效的item 数量和 无效为进行删除的item数量
func (bs *BigSet) TotalSize() int {
	bs.delLock.Lock()
	delLen := len(bs.delQueue)
	bs.delLock.Unlock()
	return bs.mapItem.sizeMap() + delLen
}

//ValidSize 当前元素的有效数量
func (bs *BigSet) ValidSize() int {
	return bs.mapItem.sizeMap()
}

//Destory 和scan 并发要求不能清理list mapdiscard
func (bs *BigSet) Destory() {
	bs.wg.Wait()
}

func (bs *BigSet) clear(qs []*Item) {
	defer bs.wg.Done()
	bs.clearLock.Lock()
	bs.listLock.Lock()
	for _, it := range qs {
		bs.delList(it)
	}
	bs.listLock.Unlock()
	bs.clearLock.Unlock()
}
