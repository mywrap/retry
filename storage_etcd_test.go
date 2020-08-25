package retry

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
)

func TestEtcdGet(t *testing.T) {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{
			"192.168.99.100:2379",
			"192.168.99.101:2379",
			"192.168.99.102:2379",
		},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("error etcd clientv3 New: %v", err)
	}
	defer etcdCli.Close()
	ctx, cxl := context.WithTimeout(context.Background(), 5*time.Second)
	ret, err := etcdCli.Get(ctx, "/retrier0/", clientv3.WithPrefix())
	cxl()
	if err != nil {
		switch err {
		case context.Canceled:
			t.Logf("canceled: %v", err)
		case context.DeadlineExceeded:
			t.Logf("timeout: %v", err)
		case rpctypes.ErrEmptyKey:
			t.Logf("client error: %v", err)
		default:
			t.Logf("bad etcd servers: %v", err)
		}
	}
	t.Logf("ret: %#v", ret)
	for _, kv := range ret.Kvs {
		t.Logf("k: %s, v: %s", kv.Key, kv.Value)
	}
}
