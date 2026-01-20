package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Value struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex
	kv map[string]Value
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		kv: make(map[string]Value),
	}
	// Your code here.
	return kv
}

// 如果 args.Key 存在，Get 返回 args.Key 对应的值和版本。
// 如果 args.Key 不存在，Get 返回 ErrNoKey。
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	key := args.Key
	kv.mu.Lock()
	v, exist := kv.kv[key]
	kv.mu.Unlock()
	if !exist {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Err = rpc.OK
		reply.Value = v.value
		reply.Version = v.version
	}
}

// 如果 args.Version 与服务器上该键的版本匹配，则更新该键的值。
// 如果版本不匹配，返回 ErrVersion。
// 如果该键不存在，当 args.Version 为 0 时，Put 会安装该值；
// 否则，返回 ErrNoKey。
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	key := args.Key
	value := args.Value
	version := args.Version
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, exist := kv.kv[key]
	if exist {
		if v.version == version {
			v.value = value
			v.version++
			kv.kv[key] = v
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if version == 0 {
			v := Value{
				value:   value,
				version: 1,
			}
			kv.kv[key] = v
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
