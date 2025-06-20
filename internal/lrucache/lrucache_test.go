package lrucache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLRUCache(t *testing.T) {
	cache := New[string, string](2)

	// First insert
	val, ok := cache.GetOrSet("a", func() (string, error) {
		return "alpha", nil
	})
	assert.False(t, ok)
	assert.Equal(t, "alpha", val)
	ruKey, ruVal := cache.RecentyUsed()
	assert.Equal(t, "a", ruKey)
	assert.Equal(t, "alpha", ruVal)
	lruKey, lruVal := cache.LeastRecentyUsed()
	assert.Equal(t, "a", lruKey)
	assert.Equal(t, "alpha", lruVal)

	// Get same key
	val, ok = cache.GetOrSet("a", func() (string, error) {
		return "should-not-be-called", nil
	})
	assert.True(t, ok)
	assert.Equal(t, "alpha", val)
	ruKey, ruVal = cache.RecentyUsed()
	assert.Equal(t, "a", ruKey)
	assert.Equal(t, "alpha", ruVal)

	// Insert second key
	cache.GetOrSet("b", func() (string, error) {
		return "bravo", nil
	})
	ruKey, ruVal = cache.RecentyUsed()
	assert.Equal(t, "b", ruKey)
	assert.Equal(t, "bravo", ruVal)
	lruKey, lruVal = cache.LeastRecentyUsed()
	assert.Equal(t, "a", lruKey)
	assert.Equal(t, "alpha", lruVal)

	// Insert third key (causes eviction of "a" if LRU)
	cache.GetOrSet("c", func() (string, error) {
		return "charlie", nil
	})
	ruKey, ruVal = cache.RecentyUsed()
	assert.Equal(t, "c", ruKey)
	assert.Equal(t, "charlie", ruVal)
	lruKey, lruVal = cache.LeastRecentyUsed()
	assert.Equal(t, "b", lruKey)
	assert.Equal(t, "bravo", lruVal)
	val, ok = cache.Get("a")
	assert.False(t, ok)

	// Confirm "b" is still there
	val, ok = cache.Get("b")
	assert.True(t, ok)
	assert.Equal(t, "bravo", val)
	ruKey, ruVal = cache.RecentyUsed()
	assert.Equal(t, "b", ruKey)
	assert.Equal(t, "bravo", ruVal)
	lruKey, lruVal = cache.LeastRecentyUsed()
	assert.Equal(t, "c", lruKey)
	assert.Equal(t, "charlie", lruVal)

	// Insert third key (causes eviction of "c" if LRU)
	cache.GetOrSet("d", func() (string, error) {
		return "delta", nil
	})
	val, ok = cache.Peek("b")
	assert.True(t, ok)
	val, ok = cache.Peek("c")
	assert.False(t, ok)

	// set
	cache.Set("d", "delta-prime")
	item, ok := cache.Peek("d")
	assert.True(t, ok)
	assert.Equal(t, "delta-prime", item)
	ruKey, ruVal = cache.RecentyUsed()
	assert.Equal(t, "d", ruKey)
	assert.Equal(t, "delta-prime", ruVal)
	lruKey, lruVal = cache.LeastRecentyUsed()
	assert.Equal(t, "b", lruKey)
	assert.Equal(t, "bravo", lruVal)
}
