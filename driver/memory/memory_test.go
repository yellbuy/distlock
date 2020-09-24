package memory

import (
	"fmt"
	"testing"
	"time"

	"github.com/yellbuy/distlock"
	"github.com/yellbuy/distlock/mutex"
)

func TestMemoryDriver_TryLock(t *testing.T) {
	var memoryDriver = New()
	locker := distlock.New(memoryDriver)
	mtx, err := locker.NewMutex("demo", mutex.Expiry(time.Second*5), mutex.Factor(0.30))
	if err != nil {
		t.Errorf("err: [%v]", err)
	}
	ok := mtx.TryLock()
	if !ok {
		t.Errorf("err: expect value:true,actual value:%v", ok)
	}
	mtx2, err := locker.NewMutex("demo", mutex.Expiry(time.Second*5), mutex.Factor(0.30))
	if err != nil {
		t.Errorf("err: [%v]", err)
	}
	ok = mtx2.TryLock()
	if ok {
		t.Errorf("err: expect value:false,actual value:%v", ok)
	}
	defer mtx2.Unlock()
	defer mtx.Unlock()
}

func TestMemoryDriver_Lock(t *testing.T) {
	var memoryDriver = New()
	locker := distlock.New(memoryDriver)
	mtx, err := locker.NewMutex("demo", mutex.Expiry(time.Second*5), mutex.Factor(0.30))
	if err != nil {
		t.Errorf("err: [%v]", err)
	}
	mtx.Lock()
	mtx2, err := locker.NewMutex("demo", mutex.Expiry(time.Second*5), mutex.Factor(0.30))
	if err != nil {
		t.Errorf("err: [%v]", err)
	}
	ok := mtx2.TryLock()
	if ok {
		t.Errorf("err: expect value:false,actual value:%v", ok)
	}
	mtx3, err := locker.NewMutex("demo", mutex.Expiry(time.Second*5), mutex.Factor(0.30))
	if err != nil {
		t.Errorf("err: [%v]", err)
	}
	mtx3.Lock()
	fmt.Println("end")
	defer mtx3.Unlock()
	defer mtx.Unlock()
	defer mtx2.Unlock()
}
