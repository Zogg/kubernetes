package etcd

import (
	"time"
	
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/runtime"
	consulstorage "k8s.io/kubernetes/pkg/storage/consul"
	
)

type APIObjectVersioner storage.APIObjectVersioner

func(v APIObjectVersioner) UpdateObject(obj runtime.Object, expiration *time.Time, resourceVersion uint64) error {
	return storage.APIObjectVersioner(v).UpdateObject(obj, expiration, resourceVersion)
}

func(v APIObjectVersioner) UpdateList(obj runtime.Object, resourceVersion uint64) error {
	return storage.APIObjectVersioner(v).UpdateList(obj, resourceVersion)
}

func(v APIObjectVersioner) ObjectResourceVersion(obj runtime.Object) (uint64, error) {
	return storage.APIObjectVersioner(v).ObjectResourceVersion(obj)
}

type EtcdStorageConfig struct {
	Config EtcdConfig
	Codec  runtime.Codec
}

func (c *EtcdStorageConfig) GetType() string {
	return "etcd"
}

func (c *EtcdStorageConfig) NewStorage() (storage.Interface, error) {
	config := &consulstorage.ConsulKvStorageConfig{
		Config: consulstorage.ConsulConfig{
			Prefix:         c.Config.Prefix,
			WaitTimeout:    time.Duration( 5 * time.Second ),
		},
		Codec:  c.Codec,
	}
	return config.NewStorage()
}


// Configuration object for constructing etcd.Config
type EtcdConfig struct {
	Prefix     string
	ServerList []string
	KeyFile    string
	CertFile   string
	CAFile     string
	Quorum     bool
}
