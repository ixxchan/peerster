package impl

import (
	"math/rand"
	"sync"
)

type neighbors struct {
	sync.RWMutex
	data []string
}

func (ns *neighbors) len() int {
	ns.Lock()
	defer ns.Unlock()

	return len(ns.data)
}

func (ns *neighbors) add(neighbor string) {
	ns.Lock()
	defer ns.Unlock()

	ns.data = append(ns.data, neighbor)
}

func (ns *neighbors) delete(neighbor string) {
	ns.Lock()
	defer ns.Unlock()

	for i, v := range ns.data {
		if v == neighbor {
			ns.data = append(ns.data[:i], ns.data[i+1:]...)
			return
		}
	}
}

func (ns *neighbors) getAll() []string {
	ns.Lock()
	defer ns.Unlock()

	return ns.data[:]
}

func (ns *neighbors) getRandom() string {
	ns.RLock()
	defer ns.RUnlock()

	return ns.data[rand.Intn(len(ns.data))]
}

func (ns *neighbors) getNewRandom(oldNeighbor string) string {
	ns.RLock()
	defer ns.RUnlock()

	for {
		neighbor := ns.data[rand.Intn(len(ns.data))]
		if neighbor != oldNeighbor {
			return neighbor
		}
	}
}

func (ns *neighbors) hasOnly(neighbor string) bool {
	ns.Lock()
	defer ns.Unlock()

	return len(ns.data) == 1 && ns.data[0] == neighbor
}

func (ns *neighbors) has(neighbor string) bool {
	ns.Lock()
	defer ns.Unlock()

	for _, v := range ns.data {
		if v == neighbor {
			return true
		}
	}
	return false
}
