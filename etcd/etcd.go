package etcd

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"strconv"
	"time"
)

type Etcd struct {
	Client *clientv3.Client
	Ctx    context.Context
}

func GetDefaultEtcdConfig() clientv3.Config {
	return clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	}
}

func InitClient(config clientv3.Config) (etcd Etcd, err error) {
	// 建立连接
	var client *clientv3.Client
	client, err = clientv3.New(config)
	etcd = Etcd{Client: client, Ctx: context.TODO()}
	return
}

func (etcd *Etcd) KvDo(op clientv3.Op) (opResponse clientv3.OpResponse, err error) {
	kv := clientv3.NewKV(etcd.Client)
	opResponse, err = kv.Do(etcd.Ctx, op)
	return
}

func (etcd *Etcd) Put(key string, value string, opts ...clientv3.OpOption) (putResponse *clientv3.PutResponse, err error) {
	putOp := clientv3.OpPut(key, value, opts...)
	opResponse, err := etcd.KvDo(putOp)
	if err != nil {
		return nil, err
	}
	putResponse = opResponse.Put()
	return putResponse, nil
}

func (etcd *Etcd) Get(key string, opts ...clientv3.OpOption) (getResponse *clientv3.GetResponse, err error) {
	getOp := clientv3.OpGet(key, opts...)
	opResponse, err := etcd.KvDo(getOp)
	if err != nil {
		return nil, err
	}
	getResponse = opResponse.Get()
	return getResponse, nil
}

func (etcd *Etcd) Delete(key string, opts ...clientv3.OpOption) (deleteResponse *clientv3.DeleteResponse, err error) {
	deleteOp := clientv3.OpDelete(key, opts...)
	opResponse, err := etcd.KvDo(deleteOp)
	if err != nil {
		return nil, err
	}
	deleteResponse = opResponse.Del()
	return deleteResponse, nil
}

func (etcd *Etcd) NewLease(ttl int64) (leaseID clientv3.LeaseID, err error) {
	lease := clientv3.NewLease(etcd.Client)
	leaseGrantResp, err := lease.Grant(etcd.Ctx, ttl)
	if err != nil {
		return 0, err
	}
	leaseID = leaseGrantResp.ID

	return leaseID, nil
}

func (etcd *Etcd) AutoRenewLease(leaseID clientv3.LeaseID) error {
	lease := clientv3.NewLease(etcd.Client)
	cancelCtx, cancelFunc := context.WithCancel(etcd.Ctx)
	defer cancelFunc()
	defer lease.Revoke(etcd.Ctx, leaseID)
	keepRespChan, err := lease.KeepAlive(cancelCtx, leaseID)
	if err != nil {
		return err
	}
	var keepResp *clientv3.LeaseKeepAliveResponse
	for {
		log("续租等待中:%v", strconv.Itoa(int(leaseID)))
		select {
		case keepResp = <-keepRespChan:
			if keepRespChan == nil || keepResp == nil {
				err = errors.New(fmt.Sprintf("%v 租约已经失效", leaseID))
				log(err.Error())
				goto END
			} else {
				log("租约续租成功: %v", keepResp.ID)
			}
		}
	}
END:
	log("end \n")
	return err
}

func (etcd *Etcd) StartTxn() (txn clientv3.Txn) {
	kvClient := clientv3.NewKV(etcd.Client)
	txn = kvClient.Txn(etcd.Ctx)
	return txn
}

func (etcd *Etcd) CommitTxn(txn clientv3.Txn) (txnResponse *clientv3.TxnResponse, err error) {
	txnResponse, err = txn.Commit()
	return
}

func (etcd *Etcd) Watch(key string, opts ...clientv3.OpOption) (cancelFunc context.CancelFunc, watchChan <-chan clientv3.WatchResponse, err error) {
	etcdWatcher := clientv3.NewWatcher(etcd.Client)
	cancelCtx, cancelFunc := context.WithCancel(etcd.Ctx)
	watchRespChan := etcdWatcher.Watch(cancelCtx, key, opts...)

	watchChann := make(chan clientv3.WatchResponse, 10)
	go func() {
		defer close(watchChann)
		log("watch go func start\n")
		for watchResp := range watchRespChan {
			watchChann <- watchResp
		}
		log("watch go func end\n")
	}()
	return cancelFunc, watchChann, nil
}

func log(format string, a ...interface{}) {
	fmt.Println(time.Now().String() + " :" + "etcd:" + fmt.Sprintf(format, a...))
}
