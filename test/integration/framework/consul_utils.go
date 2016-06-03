/*
Copyright 2014 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"github.com/golang/glog"
	consulapi "github.com/hashicorp/consul/api"
	consulstorage "k8s.io/kubernetes/pkg/storage/consul"
)

// If you need to start an consul instance by hand, you also need to insert a key
// for this check to pass (*any* key will do, eg:
//curl -L http://127.0.0.1:4001/v2/keys/message -XPUT -d value="Hello world").
func init() {
	RequireConsul()
}

func NewConsulClient() consulapi.KV {
	// TODO: Pass consul config here

	config := consulapi.DefaultConfig()
	client, err := consulapi.NewClient(config)
	if err != nil {
		glog.Fatalf("unable to connect to consul for testing: %v", err)
	}
	return client
}

/*
func NewAutoscalingConsulStorage(client consulapi) storage.Interface {
	if client == nil {
		client = NewConsulClient()
	}
	return storage.NewGenericWrapper(
		NewRawStorage(client, quorum), codec, prefix, cacheSize)
	return consulstorage.NewConsulStorage(
		client,
		testapi.Autoscaling.Codec(),
		consultest.PathPrefix(), false, consultest.DeserializationCacheSize
	)
}

func NewBatchConsulStorage(client consulapi) storage.Interface {
	if client == nil {
		client = NewConsulClient()
	}
	return consulstorage.NewConsulStorage(client, testapi.Batch.Codec(), consultest.PathPrefix(), false, consultest.DeserializationCacheSize)
}

func NewAppsConsulStorage(client consulapi) storage.Interface {
	if client == nil {
		client = NewConsulClient()
	}
	return consulstorage.NewConsulStorage(client, testapi.Apps.Codec(), consultest.PathPrefix(), false, consultest.DeserializationCacheSize)
}

func NewExtensionsConsulStorage(client consulapi) storage.Interface {
	if client == nil {
		client = NewConsulClient()
	}
	return consulstorage.NewConsulStorage(client, testapi.Extensions.Codec(), consultest.PathPrefix(), false, consultest.DeserializationCacheSize)
}
*/

func RequireConsul() {
	client := NewConsulClient()
	if _, err := client.Get("/", 0); err != nil {
		glog.Fatalf("unable to connect to consul for testing: %v", err)
	}
}

/*
func WithConsulKey(f func(string)) {
	prefix := fmt.Sprintf("/test-%d", rand.Int63())
	defer consul.NewKeysAPI(NewConsulClient()).Delete(context.TODO(), prefix, &consul.DeleteOptions{Recursive: true})
	f(prefix)
}

// DeleteAllConsulKeys deletes all keys from consul.
// TODO: Instead of sprinkling calls to this throughout the code, adjust the
// prefix in consultest package; then just delete everything once at the end
// of the test run.
func DeleteAllConsulKeys() {
	glog.Infof("Deleting all consul keys")
	keysAPI := consul.NewKeysAPI(NewConsulClient())
	keys, err := keysAPI.Get(context.TODO(), "/", nil)
	if err != nil {
		glog.Fatalf("Unable to list root consul keys: %v", err)
	}
	for _, node := range keys.Node.Nodes {
		if _, err := keysAPI.Delete(context.TODO(), node.Key, &consul.DeleteOptions{Recursive: true}); err != nil {
			glog.Fatalf("Unable delete key: %v", err)
		}
	}
}
*/
