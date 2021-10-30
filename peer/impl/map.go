package impl

import "sync"

// must not be copied since Mutex is used
type ConcurrentMap struct {
	mu sync.Mutex
	m  map[string]string
}

func NewConcurrentMap() ConcurrentMap {
	return ConcurrentMap{
		m: make(map[string]string),
	}
}

func (m *ConcurrentMap) GetMap() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.m
}

func (m *ConcurrentMap) Set(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m[key] = value
}

// if key does not exist, return ""
func (m *ConcurrentMap) Get(key string) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.m[key]
}

func (m *ConcurrentMap) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, key)
}
