package factory

import(
	"k8s.io/kubernetes/pkg/storage"
	//"k8s.io/kubernetes/pkg/storage/etcd"
	//"k8s.io/kubernetes/pkg/storage/consul"
	//"k8s.io/kubernetes/pkg/storage/generic"
)

type StorageFactory []storage.Config
