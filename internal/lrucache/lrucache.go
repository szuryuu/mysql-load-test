package lrucache

import (
	"container/list"
	"sync"
)

type entry[K comparable, V any] struct {
	key   K
	value V
}

type LRUCacheStats struct {
	HitsTotal, MissesTotal int
	EvictionsTotal         int
	MoveToFrontTotal       int
	NewItemsTotal          int
}

type LRUCache[K comparable, V any] struct {
	cache map[K]*list.Element
	list  *list.List
	size  int
	mu    sync.Mutex
	stats *LRUCacheStats
}

func New[K comparable, V any](size int) *LRUCache[K, V] {
	return &LRUCache[K, V]{
		cache: make(map[K]*list.Element),
		list:  list.New(),
		size:  size,
		stats: &LRUCacheStats{},
	}
}

func (c *LRUCache[K, V]) Stats() LRUCacheStats {
	return *c.stats
}

func (c *LRUCache[K, V]) GetOrSet(key K, fn func() (V, error)) (V, bool) {
	c.mu.Lock()
	if elem, ok := c.cache[key]; ok {
		c.stats.HitsTotal++
		c.list.MoveToFront(elem)
		c.stats.MoveToFrontTotal++
		c.mu.Unlock()
		return elem.Value.(entry[K, V]).value, true
	}
	c.stats.MissesTotal++
	c.mu.Unlock()

	newv, err := fn()
	if err != nil {
		var zero V
		return zero, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[key]; ok {
		c.stats.HitsTotal++
		c.list.MoveToFront(elem)
		c.stats.MoveToFrontTotal++
		return elem.Value.(entry[K, V]).value, true
	}

	c.stats.MissesTotal++

	if c.list.Len() >= c.size {
		back := c.list.Back()
		if back != nil {
			evicted := back.Value.(entry[K, V])
			delete(c.cache, evicted.key)
			c.list.Remove(back)
			c.stats.EvictionsTotal++
		}
	}

	e := entry[K, V]{key: key, value: newv}
	elem := c.list.PushFront(e)
	c.cache[key] = elem
	c.stats.NewItemsTotal++
	return newv, false
}

func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.cache[key]; ok {
		c.stats.HitsTotal++
		c.list.MoveToFront(elem)
		c.stats.MoveToFrontTotal++
		return elem.Value.(entry[K, V]).value, true
	}
	c.stats.MissesTotal++
	var zero V
	return zero, false
}

func (c *LRUCache[K, V]) Set(key K, val V) V {
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.cache[key]
	e := entry[K, V]{key: key, value: val}
	if ok {
		elem.Value = e
		c.list.MoveToFront(elem)
		c.stats.MoveToFrontTotal++
		return elem.Value.(entry[K, V]).value
	}
	elem = c.list.PushFront(e)
	c.stats.NewItemsTotal++
	c.cache[key] = elem
	return val
}

func (c *LRUCache[K, V]) Peek(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.cache[key]; ok {
		return elem.Value.(entry[K, V]).value, true
	}
	var zero V
	return zero, false
}

func (c *LRUCache[K, V]) RecentyUsed() (K, V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	front := c.list.Front()
	if front == nil {
		var zeroV V
		var zeroK K
		return zeroK, zeroV
	}
	return front.Value.(entry[K, V]).key, front.Value.(entry[K, V]).value
}

func (c *LRUCache[K, V]) LeastRecentyUsed() (K, V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	back := c.list.Back()
	if back == nil {
		var zeroV V
		var zeroK K
		return zeroK, zeroV
	}
	return back.Value.(entry[K, V]).key, back.Value.(entry[K, V]).value
}
