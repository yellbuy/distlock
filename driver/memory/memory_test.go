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
	mtx, err := locker.NewMutex("demo", mutex.Expiry(time.Second*5))
	if err != nil {
		t.Errorf("err: [%v]", err)
	}
	fmt.Println("mtx")
	mtx.Lock()
	mtx2, err := locker.NewMutex("demo", mutex.Expiry(time.Second*5))
	if err != nil {
		t.Errorf("err: [%v]", err)
	}
	ok := mtx2.TryLock()
	if ok {
		t.Errorf("err: expect value:false,actual value:%v", ok)
	}
	fmt.Println("mtx2")
	mtx3, err := locker.NewMutex("demo", mutex.Expiry(time.Second*5))
	if err != nil {
		t.Errorf("err: [%v]", err)
	}

	mtx3.Lock()
	fmt.Println("mtx3")
	mtx.Unlock()
	mtx4, err := locker.NewMutex("demo", mutex.Expiry(time.Second*5))
	if err != nil {
		t.Errorf("err: [%v]", err)
	}

	mtx4.Lock()
	fmt.Println("mtx4")

	fmt.Println("end")
	defer mtx4.Unlock()
	defer mtx.Unlock()
	defer mtx2.Unlock()
}
