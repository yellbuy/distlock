package memory

import (
	"sync"
	"time"
)

// https://github.com/mjaow/keylock/blob/master/lock.go
type memoryDriver struct {
	localLockMap map[string]*lock
	globalLock   sync.Mutex
}

type lock struct {
	mux        *sync.Mutex
	refCount   int
	expireTime int64
}

func New() *memoryDriver {
	return &memoryDriver{localLockMap: map[string]*lock{}}
}

func (km *memoryDriver) Lock(name, value string, expiry time.Duration) (ok bool, wait time.Duration) {
	km.globalLock.Lock()

	wl, locked := km.localLockMap[name]
	ok = false
	if !locked {
		wl = &lock{
			mux:        new(sync.Mutex),
			refCount:   0,
			expireTime: time.Now().Unix() + expiry.Nanoseconds(),
		}
		km.localLockMap[name] = wl
		ok = true
	} else {
		if wl.expireTime < time.Now().Unix() {
			wl.expireTime = time.Now().Unix() + expiry.Nanoseconds()
			km.localLockMap[name] = wl
			ok = true
		}
	}

	wl.refCount++

	km.globalLock.Unlock()

	wl.mux.Lock()
	wait = time.Duration(wl.expireTime-time.Now().Unix()) * time.Nanosecond
	return
}

func (km *memoryDriver) Unlock(name, value string) {
	km.globalLock.Lock()

	wl, locked := km.localLockMap[name]

	if !locked {
		km.globalLock.Unlock()
		return
	}

	wl.refCount--

	if wl.refCount <= 0 {
		delete(km.localLockMap, name)
	}

	km.globalLock.Unlock()

	wl.mux.Unlock()
}

func (km *memoryDriver) Touch(name, value string, expiry time.Duration) (ok bool) {
	km.globalLock.Lock()
	ok = false
	wl, locked := km.localLockMap[name]
	if locked {
		if wl.expireTime < time.Now().Unix() {
			ok = true
		}
	}
	return
}

func (km *memoryDriver) RLock(name, value string, expiry time.Duration) (ok bool, wait time.Duration) {
	panic("暂未实现")
}

func (km *memoryDriver) RUnlock(name, value string) {
	panic("未实现")
}

func (km *memoryDriver) RTouch(name, value string, expiry time.Duration) (ok bool) {
	panic("未实现")
}

func (km *memoryDriver) WLock(name, value string, expiry time.Duration) (ok bool, wait time.Duration) {
	panic("未实现")
}

func (km *memoryDriver) WUnlock(name, value string) {
	panic("未实现")
}

func (km *memoryDriver) WTouch(name, value string, expiry time.Duration) (ok bool) {
	panic("未实现")
}

func (km *memoryDriver) Watch(name string) <-chan struct{} {
	panic("未实现")
}
