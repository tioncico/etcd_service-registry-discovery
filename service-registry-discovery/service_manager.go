package service_registry_discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gin-framework/etcd"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"sync"
	"time"
)

//配置项
type Option struct {
	EtcdConfig   clientv3.Config
	Timeout      int
	RegistryPath string
}

//服务控制管理
type ServiceManager struct {
	Etcd               etcd.Etcd
	Option             Option
	lock               sync.Mutex
	ServiceMap         sync.Map
	ServiceLen         int
	CurrentNodeLeaseId clientv3.LeaseID
}

// 存储的service
type ServiceInfo struct {
	Service  Service
	NodeList map[string]Node
}

//初始化服务管理
func InitServiceManager(option Option) (*ServiceManager, error) {
	etcdClient, err := etcd.InitClient(option.EtcdConfig)
	if err != nil {
		return nil, err
	}
	serviceManager := &ServiceManager{
		Option: option,
		Etcd:   etcdClient,
	}
	return serviceManager, err
}

//注册节点
func (m *ServiceManager) RegisterNode(serviceStruct RegisterNodeStruct) error {
	_, err := m.putNodeDataEtcd(serviceStruct)
	if err != nil {
		return err
	}
	go func() {
		err = m.autoLease()
		if err != nil {
			log("自动续租出错:%v,尝试重新续租", err)
			for retryTimes := 0; retryTimes < 3; retryTimes++ {
				_, err := m.putNodeDataEtcd(serviceStruct)
				if err != nil {
					log("节点重试出错:%v", err)
					continue
				}
				err = m.autoLease()
			}
			log("%v", err)
			panic(err)
		}
	}()

	return nil
}

func (m *ServiceManager) putNodeDataEtcd(serviceStruct RegisterNodeStruct) (*clientv3.PutResponse, error) {
	nodeKey := m.Option.RegistryPath + "/node_" + serviceStruct.Node.Id
	nodeJsonByte, _ := json.Marshal(serviceStruct)
	nodeJsonStr := string(nodeJsonByte)
	leaseId, err := m.Etcd.NewLease(int64(m.Option.Timeout))
	if err != nil {
		return nil, err
	}
	m.CurrentNodeLeaseId = leaseId
	log("注册服务%v ", serviceStruct.Node.Id)
	putResponse, err := m.Etcd.Put(nodeKey, nodeJsonStr, clientv3.WithLease(leaseId))
	return putResponse, nil
}

func (m *ServiceManager) autoLease() error {
	err := m.Etcd.AutoRenewLease(m.CurrentNodeLeaseId)
	log("%v", err)
	return err
}

//初始化服务列表
func (m *ServiceManager) InitServiceList() {
	m.lock.Lock()
	defer m.lock.Unlock()

	log("初始化服务...")

	getResponse, err := m.Etcd.Get(m.Option.RegistryPath, clientv3.WithPrefix())
	if err != nil {
		panic(err)
		return
	}

	nodeLen := len(getResponse.Kvs)
	serviceMap := make(map[string]ServiceInfo, len(getResponse.Kvs))
	m.ServiceLen = 0
	for _, v := range getResponse.Kvs {
		var nodeStruct RegisterNodeStruct
		_ = json.Unmarshal(v.Value, &nodeStruct)

		if nodeStruct.Node.Id != "" {
			for _, service := range nodeStruct.ServiceList {
				serviceMapValue, ok := serviceMap[service.Name]
				if ok {
					serviceMapValue.NodeList[nodeStruct.Node.Id] = nodeStruct.Node
				} else {
					nodeList := make(map[string]Node, nodeLen)
					nodeList[nodeStruct.Node.Id] = nodeStruct.Node
					serviceMapValue = ServiceInfo{
						Service:  service,
						NodeList: nodeList,
					}
					serviceMap[service.Name] = serviceMapValue
					m.ServiceLen++
				}
				log("服务节点已获取,服务:%v  节点id:%v,host:%v:%v", serviceMapValue.Service.Name, nodeStruct.Node.Id, nodeStruct.Node.IP, nodeStruct.Node.Port)

				m.ServiceMap.Store(service.Name, serviceMapValue)
			}
		}
	}
}

//监听服务节点变化
func (m *ServiceManager) ListenNodeChange() (cancelFunc context.CancelFunc, err error) {

	cancelFunc, respChan, err := m.Etcd.Watch(m.Option.RegistryPath, clientv3.WithPrefix())

	if err != nil {
		return nil, err
	}
	for watchResp := range respChan {
		for _, event := range watchResp.Events {
			switch event.Type {
			case mvccpb.PUT:
				var nodeStruct RegisterNodeStruct
				_ = json.Unmarshal(event.Kv.Value, &nodeStruct)
				if nodeStruct.Node.Id == "" {
					continue
				}
				m.deleteNode(nodeStruct.Node.Id)
				log("节点%v发生变动", nodeStruct.Node.Id)
				for _, service := range nodeStruct.ServiceList {
					var serviceMapValue ServiceInfo
					serviceMapValueInterface, ok := m.ServiceMap.Load(service.Name)
					if ok {
						serviceMapValue = serviceMapValueInterface.(ServiceInfo)
						serviceMapValue.NodeList[nodeStruct.Node.Id] = nodeStruct.Node
					} else {
						nodeList := make(map[string]Node, 1)
						nodeList[nodeStruct.Node.Id] = nodeStruct.Node
						serviceMapValue = ServiceInfo{
							Service:  service,
							NodeList: nodeList,
						}
						m.ServiceLen++
					}
					log("服务节点已获取,服务:%v  节点id:%v,host:%v:%v", serviceMapValue.Service.Name, nodeStruct.Node.Id, nodeStruct.Node.IP, nodeStruct.Node.Port)
					m.ServiceMap.Store(service.Name, serviceMapValue)
				}
				break
			case mvccpb.DELETE:
				log(fmt.Sprintf("evet.Type: %v", string(event.Kv.Key)))
				nodeId := string(event.Kv.Key)[len(m.Option.RegistryPath+"/node_"):]
				m.deleteNode(nodeId)
				break
			}
		}
	}
	return nil, err
}

//获取一条服务的节点信息,用于请求
func (m *ServiceManager) GetServiceNode(serviceName string) (Service, Node, error) {
	serviceInfoInterface, ok := m.ServiceMap.Load(serviceName)

	if ok == false {
		return Service{}, Node{}, errors.New("未获取到该服务")
	}

	serviceInfo := serviceInfoInterface.(ServiceInfo)
	//获取一条Node信息即可
	for _, node := range serviceInfo.NodeList {
		return serviceInfo.Service, node, nil
	}
	return Service{}, Node{}, errors.New("该服务没有节点,发生异常")
}

//节点下线(删除节点相关信息)
func (m *ServiceManager) deleteNode(nodeId string) {
	serviceChangeList := make([]ServiceInfo, m.ServiceLen)
	m.ServiceMap.Range(func(key, value interface{}) bool {
		serviceInfo := value.(ServiceInfo)
		_, ok := serviceInfo.NodeList[nodeId]
		if ok {
			log("节点服务已下线,节点id:%v,服务名:%v", nodeId, serviceInfo.Service.Name)
			delete(serviceInfo.NodeList, nodeId)
			if len(serviceInfo.NodeList) == 0 {
				m.ServiceLen--
			}
			serviceChangeList = append(serviceChangeList, serviceInfo)
		}
		return true
	})
	for _, changeService := range serviceChangeList {
		nodeLen := len(changeService.NodeList)
		if nodeLen == 0 {
			m.ServiceMap.Delete(changeService.Service.Name)
		} else {
			m.ServiceMap.Store(changeService.Service.Name, changeService)
		}
	}
}

func log(format string, a ...interface{}) {
	fmt.Println(time.Now().String() + " :" + fmt.Sprintf(format, a...))
}
