package memory

import (
	"sync"
	"time"
)

// https://github.com/mjaow/keylock/blob/master/lock.go
type memoryDriver struct {
	localLockMap map[string]*lock
	globalLock   sync.Mutex
	expireTime   int64
}

type lock struct {
	mux      *sync.Mutex
	refCount int
}

func New() *memoryDriver {
	return &memoryDriver{localLockMap: map[string]*lock{}}
}

func (km *memoryDriver) Lock(name, value string, expiry time.Duration) (ok bool, wait time.Duration) {
	km.globalLock.Lock()

	wl, locked := km.localLockMap[name]
	ok = false
	curUnix := time.Now().UnixNano()
	if !locked {
		wl = &lock{
			mux:      new(sync.Mutex),
			refCount: 0,
		}
		km.expireTime = curUnix + expiry.Nanoseconds()
		km.localLockMap[name] = wl
		ok = true
	} else {
		if km.expireTime < curUnix {
			km.expireTime = curUnix + expiry.Nanoseconds()
			wl.refCount = 0
			km.localLockMap[name] = wl
			ok = true
		}
	}

	wl.refCount++

	km.globalLock.Unlock()
	wait = time.Duration(km.expireTime-curUnix) * time.Nanosecond
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
		// curUnix := time.Now().UnixNano()
		// wait := time.Duration(wl.expireTime-curUnix) * time.Nanosecond
		// time.Sleep(wait)
		delete(km.localLockMap, name)
	}

	km.globalLock.Unlock()
}

func (km *memoryDriver) Touch(name, value string, expiry time.Duration) (ok bool) {
	km.globalLock.Lock()
	ok = false
	_, locked := km.localLockMap[name]
	if locked {
		if km.expireTime < time.Now().Unix() {
			ok = true
		}
	}
	km.globalLock.Unlock()
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
	//fmt.Println("Watch ", name)
	outChan := make(chan struct{})
	//outChan <- struct{}{}
	return outChan
}
