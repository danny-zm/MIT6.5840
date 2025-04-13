package lock

import (
	"time"

	"6.5840/kvsrv/rpc"
	"6.5840/kvtest"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockKey  string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	clientID := kvtest.RandValue(8)

	lk := &Lock{
		ck:       ck,
		lockKey:  l,
		clientID: clientID,
	}

	return lk
}

func (lk *Lock) Acquire() {
	for {
		if val, version, err := lk.ck.Get(lk.lockKey); err == rpc.ErrNoKey {
			if err = lk.ck.Put(lk.lockKey, lk.clientID, 0); err == rpc.OK {
				return
			}
		} else if err == rpc.OK {
			if val == lk.clientID {
				// We already hold the lock
				return
			}

			if val == "" {
				if err = lk.ck.Put(lk.lockKey, lk.clientID, version); err == rpc.OK {
					return
				}
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		val, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey {
			return
		}

		if err == rpc.OK {
			if val == "" {
				// Lock is already free
				return
			}

			if val == lk.clientID {
				err = lk.ck.Put(lk.lockKey, "", version)
				if err == rpc.OK || err == rpc.ErrVersion {
					// Successfully released the lock or someone else modified it
					return
				}

				// err == rpc.ErrMaybe: Could be released, check again in the next iteration
			}
		}

		time.Sleep(30 * time.Millisecond)
	}
}
