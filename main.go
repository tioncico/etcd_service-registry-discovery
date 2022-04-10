package main

import (
	"gin-framework/etcd"
	serviceRegistryDiscovery "gin-framework/service-registry-discovery"
	"os"
)

func main() {
	serverId := os.Args[1]

	etcdConfig := etcd.GetDefaultEtcdConfig()

	option := serviceRegistryDiscovery.Option{
		EtcdConfig:   etcdConfig,
		Timeout:      5,
		RegistryPath: "/service-registry-discovery",
	}
	registerNodeStruct := serviceRegistryDiscovery.RegisterNodeStruct{
		Node: serviceRegistryDiscovery.Node{
			Id:     serverId,
			IP:     "127.0.0.1",
			Port:   9501,
			Weight: 1,
		},
		ServiceList: []serviceRegistryDiscovery.Service{
			{Name: "register"},
			{Name: "login"},
			{Name: "test"},
		},
	}
	serviceManager, err := serviceRegistryDiscovery.InitServiceManager(option)
	if err != nil {
		panic(err)
	}
	err = serviceManager.RegisterNode(registerNodeStruct)
	if err != nil {
		panic(err)
	}
	serviceManager.InitServiceList()
	_, _ = serviceManager.ListenNodeChange()
}

//
//func curd() {
//	// 客户端配置
//	config := clientv3.Config{
//		Endpoints:   []string{"127.0.0.1:2379"},
//		DialTimeout: 5 * time.Second,
//	}
//	etcdClient, err := etcd.InitClient(config)
//	if err != nil {
//		fmt.Printf("%v \n", err)
//		return
//	}
//	put, err := etcdClient.Put("test", "123")
//	fmt.Printf("%v \n", put)
//	get, err := etcdClient.Get("test")
//	fmt.Printf("%v \n", get)
//	del, err := etcdClient.Delete("test")
//	fmt.Printf("%v \n", del)
//
//	_, _ = etcdClient.NewLease(5, etcd.AUTO_RENEW_ON)
//
//}
//
//func watch() {
//	// 客户端配置
//	config := clientv3.Config{
//		Endpoints:   []string{"127.0.0.1:2379"},
//		DialTimeout: 5 * time.Second,
//	}
//	etcdClient, err := etcd.InitClient(config)
//	if err != nil {
//		fmt.Printf("%v \n", err)
//		return
//	}
//	go func() {
//		cancelFunc, respChan, err := etcdClient.Watch("test/", clientv3.WithPrefix())
//		if err != nil {
//			panic(err)
//		}
//
//		go func() {
//			for watchResp := range respChan {
//				fmt.Printf("%v \n", watchResp)
//				for _, event := range watchResp.Events {
//					switch event.Type {
//					case mvccpb.PUT:
//						fmt.Println("修改为:", string(event.Kv.Key))
//					case mvccpb.DELETE:
//						fmt.Println("删除了", "Revision:", event.Kv.Key)
//					}
//				}
//			}
//		}()
//
//		time.Sleep(10 * time.Second)
//		cancelFunc()
//	}()
//	go func() {
//		for {
//			_, err = etcdClient.Put("test/"+time.Now().String(), time.Now().String())
//			if err != nil {
//				panic(err)
//			}
//
//			time.Sleep(1 * time.Second)
//		}
//	}()
//}
