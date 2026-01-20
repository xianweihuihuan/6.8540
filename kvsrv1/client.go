package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get 获取一个键的当前值和版本。如果键不存在，返回 ErrNoKey。
// 在遇到其他所有错误时，它会一直重试，直到成功。

// 你可以用如下代码发送 RPC：
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)

// args 和 reply 的类型（包括它们是否为指针）必须与 RPC 处理函数的参数类型匹配。
// 此外，reply 必须作为指针传递。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	GetArgs := rpc.GetArgs{
		Key: key,
	}
	GetReply := rpc.GetReply{}
	for ok := ck.clnt.Call(ck.server, "KVServer.Get", &GetArgs, &GetReply); !ok; {
		ok = ck.clnt.Call(ck.server, "KVServer.Get", &GetArgs, &GetReply)
		time.Sleep(100 * time.Millisecond)
	}

	return GetReply.Value, GetReply.Version, GetReply.Err
	//return "", 0, rpc.ErrNoKey
}

// 只有当请求中的版本与服务器上该键的版本匹配时，才更新键的值。
// 如果版本号不匹配，服务器应该返回 ErrVersion。
// 如果 Put 在第一次 RPC 调用时收到 ErrVersion，Put 应该返回 ErrVersion，
// 因为该 Put 操作显然没有在服务器上执行。
// 如果服务器在重新发送的 RPC 中返回 ErrVersion，
// 则 Put 必须返回 ErrMaybe 给应用程序，
// 因为其早期的 RPC 可能已经成功执行，但响应丢失，
// 且 Clerk 无法知道该 Put 操作是否成功执行。

// 你可以用如下代码发送 RPC：
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)

// args 和 reply 的类型（包括它们是否为指针）必须与 RPC 处理函数的参数类型匹配。
// 此外，reply 必须作为指针传递。
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	PutArgs := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	PutReply := rpc.PutReply{}
	i := 1
	ok := ck.clnt.Call(ck.server,"KVServer.Put",&PutArgs,&PutReply)
	for !ok {
		time.Sleep(time.Millisecond * 100)
		ok = ck.clnt.Call(ck.server,"KVServer.Put",&PutArgs,&PutReply)
		i++
	}
	if i != 1 && PutReply.Err == rpc.ErrVersion{
		return rpc.ErrMaybe
	}
	return PutReply.Err
	//return rpc.ErrNoKey
}
