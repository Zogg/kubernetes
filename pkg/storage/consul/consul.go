package consul

import {
  "errors"
  //"time"
  
  "k8s.io/kubernetes/pkg/conversion"
  "k8s.io/kbuernetes/pkg/runtime"
  "k8s.io/kubernetes/pkg/storage"
  "k8s.io/kubernetes/pkg/util"
  //"k8s.io/kubernetes/pkg/watch"
  
  consulapi "github.com/hashicorp/consul/api"
  "golang.org/x/net/context"
}

type ConsulKvStorageConfig struct {
  Config      ConsulConfig
  Codec       runtime.Codec
}

// implements storage.Config
func (c *ConsulKvStorageConfig) GetType() string {
  return "consulkv"
}

// implements storage.Config
func (c *ConsulKvStorageConfig) NewStorage() (storage.Interface, error) {
  return newConsulKvStorage( c.Config, c.Codec )
}

type ConsulConfig struct {
  // TODO add specific configuration values for k8s to pass to consul client
}

func (c *ConsulConfig)  getConsulApiConfig() consulapi.Config {
  config := consulapi.DefaultConfig()
  
  // TODO do stuff to propagate configuration values from our structure
  // to theirs
  
  return config
}


func newConsulKvStorage(config *ConsulConfig, codec runtime.Codec) (ConsulKvStorage, error) {
  client, err := consulapi.NewClient(config.getConsulApiConfig())
  if err != nil {
    return nil, err
  }
  return ConsulKvStorage {
    ConsulKv:   client.KV(),
    codec:      codec,
    versioner:  nil, // TODO
    copier:     api.Scheme,
  }
}

type ConsulKvStorage struct {
  ConsulKv    consulapi.KV
  codec       runtime.Codec
  copier      runtime.ObjectCopier
  versioner   storage.Versioner
  pathPrefix  string
}

func (s *ConsulKvStorage) Codec() runtime.Codec {
  return s.codec
}

func (s *ConsulKvStorage) Backends(ctx context.Context) []string {
  // TODO
  return []string{}
}

func (s *ConsulKvStorage) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
  trace := util.NewTrace("ConsulKvStorage::Create " + getTypeName(obj))
  defer trace.LogIfLong(250 * time.Millisecond)
  if ctx == nil {
    glog.Errorf("Context is nil")
  }
  key = s.prefixKey(key)
  data, err := runtime.Encode(s.codec, obj)
  trace.Step("Object encoded")
  if err != nil {
    return err
  }
  if version, err := s.versioner.ObjectResourceVersion(obj); err == nil && version != 0 {
    return errors.New("resourceVersion may not be set on objects to be created")
  }
  trace.Step("Version checked")
  
  // TODO: metrics and stuff
  // startTime := time.Now()
  kv := &consulapi.KVPair{
    Key:            key,
    Value:          data,
    ModifyIndex:    0,    // explicitly set to indicate Create-Only behavior
    // TODO: TTL, if and when this functionality becomes available
  }
  if out == nil {
    succeeded, _, err := s.ConsulKv.CAS( kv, nil )
  } else {
    succeeded, _, err := s.ConsulKv.AcquireCAS( kv, 
  }
  // metrics.RecordStuff
  trace.Step("Object created")
  if err != nil {
    return translateErrConsulKvToStorage( err, key, 0 )
  }
  if out != nil {
    if _, err := conversion.EnforcePtr(out); err != nil {
      panic("unable to convert output object to pointer")
    }
    _, _, err = s.extractObj(metaResponse, out, false, false)
  }
  return err
}

func (s *ConsulKvStorage) Set(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
  
}

func (s *ConsulKvStorage) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions) error {
  
}

func (s *ConsulKvStorage) Watch(ctx context.Context, key string, resourceVersion string, filter storage.FilterFunc) (watch.Interface, error) {
  
}

func (s *ConsulKvStorage) WatchList(ctx context.Context, key string, resourceVersion string, filter storage.FilterFunc) (watch.Interface, error) {
  
}

func (s *ConsulKvStorage) Get(ctx context.Context, key string, objPtr runtime.Object, ignoreNotFound bool) error {
  
}

func (s *ConsulKvStorage) GetToList(ctx context.Context, key string, filter storage.FilterFunc, listObj runtime.Object) error {
  
}

func (s *ConsulKvStorage) List(ctx context.Context, key string, resourceVersion string, filter storage.FilterFunc, listObj runtime.Object) error {
  
}

func (s *ConsulKvStorage) GuaranteedUpdate(ctx context.Context, key string, ptrToType runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc) error {
  
}


func (s *ConsulKvStorage) prefixKey(key string) string {
  if strings.HasPrefix(key, s.pathPrefix) {
    return key
  }
  return path.Join(s.pathPrefix, key)
}

