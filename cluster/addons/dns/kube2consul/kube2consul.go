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

// kube2consul is a bridge between Kubernetes and SkyDNS.  It watches the
// Kubernetes master for changes in Services and manifests them into consul
// to serve as DNS records.
package main

import (
	"fmt"
	etcd "github.com/coreos/go-etcd/etcd"
	consulApi "github.com/hashicorp/consul/api"
	kcache "k8s.io/kubernetes/pkg/client/cache"
	"strings"
	"sync"
	"time"
)

type etcdClient interface {
	Set(path, value string, ttl uint64) (*etcd.Response, error)
	RawGet(key string, sort, recursive bool) (*etcd.RawResponse, error)
	Delete(path string, recursive bool) (*etcd.Response, error)
}

type kube2consul struct {
	// TODO: Abstract this so it allows consul or etcd K/V storages.
	// Etcd client.
	etcdClient etcdClient
	// DNS domain name.
	domain string
	// Consul mutation timeout.
	consulMutationTimeout time.Duration
	// A cache that contains all the endpoints in the system.
	endpointsStore kcache.Store
	// A cache that contains all the services in the system.
	servicesStore kcache.Store
	// A cache that contains all the pods in the system.
	podsStore kcache.Store
	// Lock for controlling access to headless services.
	mlock sync.Mutex
}

func sanitizeIP(ip string) string {
	return strings.Replace(ip, ".", "-", -1)
}

func buildDNSNameString(labels ...string) string {
	var res string
	for _, label := range labels {
		if res == "" {
			res = label
		} else {
			res = fmt.Sprintf("%s.%s", label, res)
		}
	}
	return res
}

func (kc *kube2consul) newService(obj interface{}) {
	_, err := consulApi.NewClient(consulApi.DefaultConfig())
	if err != nil {
		panic(err)
	}
}

func (ks *kube2consul) updateService(oldObj, newObj interface{}) {
}

func (kc *kube2consul) removeService(obj interface{}) {
}

func (kc *kube2consul) handleEndpointAdd(obj interface{}) {
}

func (ks *kube2consul) handlePodCreate(obj interface{}) {
}

func (ks *kube2consul) handlePodUpdate(old interface{}, new interface{}) {
}

func (ks *kube2consul) handlePodDelete(obj interface{}) {
}
