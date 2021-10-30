package impl

import "sync"

// must not be copied since Mutex is used
type ConcurrentMapString struct {
	mu sync.Mutex
	m  map[string]string
}

func NewConcurrentMapString() ConcurrentMapString {
	return ConcurrentMapString{
		m: make(map[string]string),
	}
}

func (m *ConcurrentMapString) GetMap() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.m
}

func (m *ConcurrentMapString) Set(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m[key] = value
}

// if key does not exist, return ""
func (m *ConcurrentMapString) Get(key string) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.m[key]
}

func (m *ConcurrentMapString) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, key)
}

// must not be copied since Mutex is used
type ConcurrentMapSet struct {
	mu sync.Mutex
	m  map[string]map[string]struct{}
}

func NewConcurrentMapSet() ConcurrentMapSet {
	return ConcurrentMapSet{
		m: make(map[string]map[string]struct{}),
	}
}

func (m *ConcurrentMapSet) GetMap() map[string]map[string]struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.m
}

// Select a random value from the set. If the set is empty, return "".
func (m *ConcurrentMapSet) GetRandom(key string) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	for value := range m.m[key] {
		return value
	}
	return ""
}

func (m *ConcurrentMapSet) Add(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	set, ok := m.m[key]
	if !ok {
		set = make(map[string]struct{})
		m.m[key] = set
	}

	set[value] = struct{}{}
}
