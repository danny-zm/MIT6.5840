package lock

import (
	"6.5840/kvtest"
	"6.5840/shardkv/kvsrv"
)

type Lock struct {
	ck *kvsrv.Clerk
}

// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck.(*kvsrv.Clerk)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// You may add code here.
}

func (lk *Lock) Release() {
	// You may add code here.
}
