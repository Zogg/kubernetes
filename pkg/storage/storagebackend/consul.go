/*
Copyright 2016 The Kubernetes Authors All rights reserved.

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

package storagebackend

import (
	"strings"

	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/consul"
	consulapi "github.com/hashicorp/consul/api"
	"k8s.io/kubernetes/pkg/storage/generic"
)

func newConsulStorage(c Config) (storage.Interface, error) {
	endpoints := c.ServerList
	for i, s := range endpoints {
		endpoints[i] = strings.TrimLeft(s, "http://")
	}

	raw, err := newConsulRawStorage(c)
	if err != nil {
		return nil, err
	}
	return storage.NewGenericWrapperInt(raw, c.Codec, c.Prefix), nil
}

func newConsulRawStorage(c Config) (generic.InterfaceRaw, error) {
	client, err := consulapi.NewClient(c.getConsulApiConfig())
	if err != nil {
		return nil, err
	}
	raw := &consul.ConsulKvStorage {
		ConsulKv:   *client.KV(),
		// TODO: make this configurable for multiple servers
		ServerList:     []string{c.getConsulApiConfig().Address},
		WaitTimeout: consul.DefaultWaitTimeout,
	}
	return raw, nil
}