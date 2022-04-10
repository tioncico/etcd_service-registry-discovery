package service_registry_discovery

type Service struct {
	Name string `json:"name"`
}

type Node struct {
	Id     string `json:"id"`
	IP     string `json:"ip"`
	Port   int    `json:"port"`
	Weight int    `json:"weight"`
}

type RegisterNodeStruct struct {
	Node        Node
	ServiceList []Service
}
