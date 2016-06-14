// +build integration,!no-etcd

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

package integration

import (
	consulapi "github.com/hashicorp/consul/api"
	"golang.org/x/net/context"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/testapi"
	"k8s.io/kubernetes/pkg/runtime"
	//	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/consul/consultest"
	storagebackend "k8s.io/kubernetes/pkg/storage/storagebackend"
	//	"k8s.io/kubernetes/pkg/watch"
	"k8s.io/kubernetes/test/integration/framework"
	"testing"
)

func TestConsulCreate(t *testing.T) {
	serverList := []string{"http://localhost"}
	config := storagebackend.Config{
		Type:       storagebackend.StorageTypeConsul,
		Codec:      testapi.Default.Codec(),
		ServerList: serverList,
		Prefix:     consultest.PathPrefix(),
		DeserializationCacheSize: consultest.DeserializationCacheSize,
	}

	cstorage, err := storagebackend.Create(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ctx := context.TODO()

	framework.WithConsulKey(func(key string) {
		testObject := api.ServiceAccount{ObjectMeta: api.ObjectMeta{Name: "foo"}}
		err = cstorage.Create(ctx, key, &testObject, nil, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		consulClient := framework.NewConsulClient()

		prefixedKey := consultest.AddPrefix(key)

		kvPair, _, err := consulClient.Get(prefixedKey, nil)

		if kvPair == nil {
			t.Fatalf("Key %v not found", key)
		}

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		decoded, err := runtime.Decode(testapi.Default.Codec(), []byte(kvPair.Value))
		if err != nil {
			t.Fatalf("unexpected response: %#v", kvPair.Value)
		}
		result := *decoded.(*api.ServiceAccount)

		// Propagate ResourceVersion (it is set automatically).
		testObject.ObjectMeta.ResourceVersion = result.ObjectMeta.ResourceVersion
		if !api.Semantic.DeepEqual(testObject, result) {
			t.Errorf("expected: %#v got: %#v", testObject, result)
		}
	})
}

func TestConsulGet(t *testing.T) {
	serverList := []string{"http://localhost"}
	config := storagebackend.Config{
		Type:       storagebackend.StorageTypeConsul,
		Codec:      testapi.Default.Codec(),
		ServerList: serverList,
		Prefix:     consultest.PathPrefix(),
		DeserializationCacheSize: consultest.DeserializationCacheSize,
	}

	cstorage, err := storagebackend.Create(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ctx := context.TODO()

	framework.WithConsulKey(func(key string) {
		testObject := api.ServiceAccount{ObjectMeta: api.ObjectMeta{Name: "foo"}}
		coded, err := runtime.Encode(testapi.Default.Codec(), &testObject)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		consulClient := framework.NewConsulClient()
		prefixedKey := consultest.AddPrefix(key)
		kvPair := &consulapi.KVPair{
			Key:         prefixedKey,
			Value:       coded,
			ModifyIndex: 0,
		}
		_, err = consulClient.Put(kvPair, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		result := api.ServiceAccount{}
		if err := cstorage.Get(ctx, key, &result, false); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Propagate ResourceVersion (it is set automatically).
		testObject.ObjectMeta.ResourceVersion = result.ObjectMeta.ResourceVersion
		if !api.Semantic.DeepEqual(testObject, result) {
			t.Errorf("expected: %#v got: %#v", testObject, result)
		}
	})
}

/*
// TODO: Implement this once consul has ttls
func TestConsulWriteTTL(t *testing.T) {
	client := framework.NewConsulClient()
	keysAPI := etcd.NewKeysAPI(client)
	consulstorage := consulstorage.NewConsulStorage(client, testapi.Default.Codec(), "", false, etcdtest.DeserializationCacheSize)
	ctx := context.TODO()
	framework.WithConsulKey(func(key string) {
		testObject := api.ServiceAccount{ObjectMeta: api.ObjectMeta{Name: "foo"}}
		if err := consulstorage.Create(ctx, key, &testObject, nil, 0); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		result := &api.ServiceAccount{}
		err := consulstorage.GuaranteedUpdate(ctx, key, result, false, nil, func(obj runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			if in, ok := obj.(*api.ServiceAccount); !ok || in.Name != "foo" {
				t.Fatalf("unexpected existing object: %v", obj)
			}
			if res.TTL != 0 {
				t.Fatalf("unexpected TTL: %#v", res)
			}
			ttl := uint64(10)
			out := &api.ServiceAccount{ObjectMeta: api.ObjectMeta{Name: "out"}}
			return out, &ttl, nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Name != "out" {
			t.Errorf("unexpected response: %#v", result)
		}
		if res, err := keysAPI.Get(ctx, key, nil); err != nil || res == nil || res.Node.TTL != 10 {
			t.Fatalf("unexpected get: %v %#v", err, res)
		}

		result = &api.ServiceAccount{}
		err = consulstorage.GuaranteedUpdate(ctx, key, result, false, nil, func(obj runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			if in, ok := obj.(*api.ServiceAccount); !ok || in.Name != "out" {
				t.Fatalf("unexpected existing object: %v", obj)
			}
			if res.TTL <= 1 {
				t.Fatalf("unexpected TTL: %#v", res)
			}
			out := &api.ServiceAccount{ObjectMeta: api.ObjectMeta{Name: "out2"}}
			return out, nil, nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Name != "out2" {
			t.Errorf("unexpected response: %#v", result)
		}
		if res, err := keysAPI.Get(ctx, key, nil); err != nil || res == nil || res.Node.TTL <= 1 {
			t.Fatalf("unexpected get: %v %#v", err, res)
		}
	})
}
*/

/*
func TestConsulWatch(t *testing.T) {
	serverList := []string{"http://localhost"}
	config := storagebackend.Config{
		Type:       storagebackend.StorageTypeConsul,
		Codec:      testapi.Default.Codec(),
		ServerList: serverList,
		Prefix:     consultest.PathPrefix(),
		DeserializationCacheSize: consultest.DeserializationCacheSize,
	}

	cstorage, err := storagebackend.Create(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ctx := context.TODO()
	consulClient := framework.NewConsulClient()
	coded := runtime.EncodeOrDie(testapi.Default.Codec(), &api.Pod{ObjectMeta: api.ObjectMeta{Name: "foo"}})

	framework.WithConsulKey(func(key string) {
		prefixedKey := consultest.AddPrefix(key)

		kvPair := &consulapi.KVPair{
			Key:         prefixedKey,
			Value:       []byte(coded),
			ModifyIndex: 0,
		}
		_, err = consulClient.Put(kvPair, nil)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		w, err := cstorage.Watch(ctx, key, "0", storage.Everything)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		event := <-w.ResultChan()
		if event.Type != watch.Added || event.Object == nil {
			t.Fatalf("expected first value to be set to ADDED, got %#v", event)
		}

		// should be no events in the stream
		select {
		case event, ok := <-w.ResultChan():
			if !ok {
				t.Fatalf("channel closed unexpectedly")
			}
			t.Fatalf("unexpected object in channel: %#v", event)
		default:
		}

		// should return the previously deleted item in the watch, but with the latest index
		testObject := api.ServiceAccount{ObjectMeta: api.ObjectMeta{Name: "foo"}}
		err = cstorage.Delete(ctx, key, &testObject, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		event = <-w.ResultChan()
		if event.Type != watch.Deleted {
			t.Errorf("expected deleted event %#v", event)
		}
		w.Stop()
	})
}
*/
