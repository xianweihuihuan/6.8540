package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk 是一个用于键值（k/v）客户端的 Go 接口：该接口隐藏了 ck 的具体类型
	// 但是保证 ck 支持 Put 和 Get 操作。测试器在调用 MakeLock() 时会传入该客户端（clerk）。
	ck kvtest.IKVClerk
	// You may add code here
	lockkey  string
	clientID string
}

// 测试器调用 MakeLock() 并传入一个键值（k/v）客户端；你的代码可以通过调用 lk.ck.Put() 或 lk.ck.Get() 来执行 Put 或 Get 操作。
//
// 使用 l 作为键来存储“锁的状态”（你需要决定锁的状态具体是什么）。
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	ClientID := kvtest.RandValue(8)
	lk := &Lock{
		ck:       ck,
		lockkey:  l,
		clientID: ClientID,
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.lockkey)
		if err == rpc.ErrNoKey {
			perr := lk.ck.Put(lk.lockkey, lk.clientID, 0)
			if perr == rpc.OK {
				break
			}
		} else if value == lk.clientID {
			break
		} else if value == "" {
			perr := lk.ck.Put(lk.lockkey, lk.clientID, version)
			if perr == rpc.OK {
				break
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.lockkey)
		if err == rpc.ErrNoKey {
			return
		} else if value == "" {
			break
		} else if value != lk.clientID {
			break
		} else if value == lk.clientID {
			perr := lk.ck.Put(lk.lockkey, "", version)
			if perr == rpc.OK {
				break
			}
		}
	}
}
