/*
Copyright 2015 The Kubernetes Authors All rights reserved.

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

package main

import (
	"fmt"
	consulApi "github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/cache"
	"testing"
)

type fakeConsulAgent struct {
	// TODO: Convert this to real fs to better simulate consul behavior.
	writes map[string]string
}

func (fa *fakeConsulAgent) ServiceRegister(service *consulApi.AgentServiceRegistration) error {
	key := fmt.Sprintf("%s", service.ID)
	value := fmt.Sprintf("%s:%d", service.Address, service.Port)
	fa.writes[key] = value
	return nil
}

func (fa *fakeConsulAgent) ServiceDeregister(serviceID string) error {
	delete(fa.writes, serviceID)
	return nil
}

// FIXME: instead of duplicating, extract these methods from kube2sky tests

type hostPort struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func getHostPort(service *kapi.Service) *hostPort {
	return &hostPort{
		Host: service.Spec.ClusterIP,
		Port: int(service.Spec.Ports[0].Port),
	}
}
func newService(namespace, serviceName, clusterIP, portName string, portNumber int) kapi.Service {
	service := kapi.Service{
		ObjectMeta: kapi.ObjectMeta{
			Name:      serviceName,
			Namespace: namespace,
		},
		Spec: kapi.ServiceSpec{
			ClusterIP: clusterIP,
			Ports: []kapi.ServicePort{
				{Port: int32(portNumber), Name: portName, Protocol: "TCP"},
			},
		},
	}
	return service
}

const (
	testDomain       = "cluster.local."
	basePath         = "/skydns/local/cluster"
	serviceSubDomain = "svc"
	podSubDomain     = "pod"
)

type fakeConsulKV struct {
	writes map[string]*consulApi.KVPair
}

func (fk *fakeConsulKV) Get(key string, q *consulApi.QueryOptions) (*consulApi.KVPair, *consulApi.QueryMeta, error) {
	pair := fk.writes[key]
	return pair, nil, nil
}

func (fk *fakeConsulKV) Put(p *consulApi.KVPair, q *consulApi.WriteOptions) (*consulApi.WriteMeta, error) {
	key := p.Key
	fk.writes[key] = p
	return nil, nil
}

func (fk *fakeConsulKV) Delete(key string, w *consulApi.WriteOptions) (*consulApi.WriteMeta, error) {
	delete(fk.writes, key)
	return nil, nil
}

func newKube2Consul(ca *fakeConsulAgent, ck *fakeConsulKV) *kube2consul {
	return &kube2consul{
		consulAgent:    ca,
		consulKV:       ck,
		domain:         testDomain,
		endpointsStore: cache.NewStore(cache.MetaNamespaceKeyFunc),
		servicesStore:  cache.NewStore(cache.MetaNamespaceKeyFunc),
	}
}

func TestAddSinglePortService(t *testing.T) {
	const (
		testService   = "testservice"
		testNamespace = "default"
	)
	fck := &fakeConsulKV{make(map[string]*consulApi.KVPair)}
	fca := &fakeConsulAgent{make(map[string]string)}
	k2c := newKube2Consul(fca, fck)
	service := newService(testNamespace, testService, "1.2.3.4", "", 0)
	k2c.newService(&service)
	// expectedValue := getHostPort(&service)

	assert.Equal(t, 1, len(fck.writes))
}
