package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	state string 
	id string 
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, state: l, id: kvtest.RandValue(8)}
	_, ver, err := lk.ck.Get(lk.state)
	if err == rpc.ErrNoKey {
		lk.ck.Put(lk.state, "", ver)
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, _ := lk.ck.Get(lk.state)
		if value == "" {
			err := lk.ck.Put(lk.state, lk.id, version)
			if err == rpc.OK {
				break
			}
		} else {
			if value == lk.id {
				break 
			}
		}
	}
	// Your code here
}

func (lk *Lock) Release() {
	_, version, _ := lk.ck.Get(lk.state)
	lk.ck.Put(lk.state, "", version)
	// Your code here
}
