package glob

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

type TryLocker struct {
	sync.Mutex
}

func (m *TryLocker) Lock() bool {
	return atomic.CompareAndSwapInt32((*int32)(unsafe.Pointer(&m.Mutex)), 0, 1)
}
