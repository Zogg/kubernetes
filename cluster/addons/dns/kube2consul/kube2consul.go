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
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	etcd "github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
	consulApi "github.com/hashicorp/consul/api"
	flag "github.com/spf13/pflag"
	bridge "k8s.io/kubernetes/cluster/addons/dns/bridge"
	kapi "k8s.io/kubernetes/pkg/api"
	kcache "k8s.io/kubernetes/pkg/client/cache"
	kclient "k8s.io/kubernetes/pkg/client/unversioned"
	kframework "k8s.io/kubernetes/pkg/controller/framework"
	utilflag "k8s.io/kubernetes/pkg/util/flag"
	"k8s.io/kubernetes/pkg/util/wait"
	"net/url"
)

const (
	// Maximum number of attempts to connect to consul server.
	maxConnectAttempts = 12
	// Resync period for the kube controller loop.
	resyncPeriod = 30 * time.Minute
	// A subdomain added to the user specified domain for all services.
	serviceSubdomain = "svc"
	// A subdomain added to the user specified dmoain for all pods.
	podSubdomain = "pod"
)

var (
	argDomain              = flag.String("domain", "cluster.local", "domain under which to create names")
	argEtcdMutationTimeout = flag.Duration("etcd-mutation-timeout", 10*time.Second, "crash after retrying etcd mutation for a specified duration")
	argEtcdServer          = flag.String("etcd-server", "http://127.0.0.1:4001", "URL to etcd server")
	argKubeMasterURL       = flag.String("kube-master-url", "", "URL to reach kubernetes master. Env variables in this flag will be expanded.")
	argKubecfgFile         = flag.String("kubecfg-file", "", "Location of kubecfg file for access to kubernetes master service; --kube-master-url overrides the URL part of this; if neither this nor --kube-master-url are provided, defaults to service account tokens")
	argConsulAgent         = flag.String("consul-agent", "http://127.0.0.1:8500", "URL to consul agent")
	healthzPort            = flag.Int("healthz-port", 8081, "port on which to serve a kube2sky HTTP readiness probe.")
)

type etcdClient interface {
	Set(path, value string, ttl uint64) (*etcd.Response, error)
	RawGet(key string, sort, recursive bool) (*etcd.RawResponse, error)
	Delete(path string, recursive bool) (*etcd.Response, error)
}

type consulAgent interface {
	ServiceRegister(service *consulApi.AgentServiceRegistration) error
}

type kube2consul struct {
	// TODO: Abstract this so it allows consul or etcd K/V storages.
	// Etcd client.
	// TODO: remove.
	etcdClient etcdClient

	// Consul client agent.
	consulAgent consulAgent
	// DNS domain name.
	domain string
	// Etcd mutation timeout.
	etcdMutationTimeout time.Duration
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

func (ks *kube2consul) addDNS(record string, service *kapi.Service) error {
	if strings.Contains(record, ".") {
		glog.V(1).Infof("Service names containing '.' are not supported: %s\n", service.Name)
		return nil
	}

	for i := range service.Spec.Ports {
		asr := &consulApi.AgentServiceRegistration{
			ID:      record,
			Name:    record,
			Address: service.Spec.ClusterIP,
			Port:    service.Spec.Ports[0].Port,
		}

		glog.V(2).Infof("Setting DNS record: %v -> %d\n", record, service.Spec.Ports[i].Port)

		if err := ks.consulAgent.ServiceRegister(asr); err != nil {
			return err
		}
	}
	return nil
}
func (kc *kube2consul) removeDNS(subdomain string) error {
	glog.V(2).Infof("Removing %s from DNS", subdomain)
	resp, err := kc.etcdClient.Get(skymsg.Path(subdomain), false, true)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNotFound {
		glog.V(2).Infof("Subdomain %q does not exist in etcd", subdomain)
		return nil
	}
	_, err = kc.etcdClient.Delete(skymsg.Path(subdomain), true)
	return err
}

func (kc *kube2consul) newService(obj interface{}) {
	_, err := consulApi.NewClient(consulApi.DefaultConfig())
	if err != nil {
		panic(err)
	}

	if s, ok := obj.(*kapi.Service); ok {
		name := bridge.BuildDNSNameString(s.Name, s.Namespace)
		if err := kc.addDNS(s.Name, s); err != nil {
			glog.V(1).Infof("Failed to add service: %v due to: %v", name, err)
		}
	}
}

func newConsulClient(consulAgent string) (*consulApi.Client, error) {
	var (
		client *consulApi.Client
		err    error
	)

	consulConfig := consulApi.DefaultConfig()
	consulAgentUrl, err := url.Parse(consulAgent)
	if err != nil {
		glog.Infof("Error parsing Consul url")
		return nil, err
	}

	if consulAgentUrl.Host != "" {
		consulConfig.Address = consulAgentUrl.Host
	}

	if consulAgentUrl.Scheme != "" {
		consulConfig.Scheme = consulAgentUrl.Scheme
	}

	client, err = consulApi.NewClient(consulConfig)
	if err != nil {
		glog.Infof("Error creating Consul client")
		return nil, err
	}

	for attempt := 1; attempt <= maxConnectAttempts; attempt++ {
		if _, err = client.Agent().Self(); err == nil {
			break
		}

		if attempt == maxConnectAttempts {
			break
		}

		glog.Infof("[Attempt: %d] Attempting access to Consul after 5 second sleep", attempt)
		time.Sleep(5 * time.Second)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to Consul agent: %v, error: %v", consulAgent, err)
	}
	glog.Infof("Consul agent found: %v", consulAgent)

	return client, nil
}

// setupSignalHandlers runs a goroutine that waits on SIGINT or SIGTERM and logs it
// before exiting.
func setupSignalHandlers() {
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	// This program should always exit gracefully logging that it received
	// either a SIGINT or SIGTERM. Since kube2sky is run in a container
	// without a liveness probe as part of the kube-dns pod, it shouldn't
	// restart unless the pod is deleted. If it restarts without logging
	// anything it means something is seriously wrong.
	// TODO: Remove once #22290 is fixed.
	go func() {
		glog.Fatalf("Received signal %s", <-sigChan)
	}()
}

func (ks *kube2consul) updateService(oldObj, newObj interface{}) {
}

func (kc *kube2consul) removeService(obj interface{}) {
}

func (kc *kube2consul) addDNSUsingEndpoints(subdomain string, e *kapi.Endpoints) error {
	kc.mlock.Lock()
	defer kc.mlock.Unlock()
	svc, err := kc.getServiceFromEndpoints(e)
	if err != nil {
		return err
	}
	if svc == nil || kapi.IsServiceIPSet(svc) {
		// No headless service found corresponding to endpoints object.
		return nil
	}
	// Remove existing DNS entry.
	if err := kc.removeDNS(subdomain); err != nil {
		return err
	}
	return kc.generateRecordsForHeadlessService(subdomain, e, svc)
}

func (kc *kube2consul) getServiceFromEndpoints(e *kapi.Endpoints) (*kapi.Service, error) {
	key, err := kcache.MetaNamespaceKeyFunc(e)
	if err != nil {
		return nil, err
	}
	obj, exists, err := kc.servicesStore.GetByKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get service object from services store - %v", err)
	}
	if !exists {
		glog.V(1).Infof("could not find service for endpoint %q in namespace %q", e.Name, e.Namespace)
		return nil, nil
	}
	if svc, ok := obj.(*kapi.Service); ok {
		return svc, nil
	}
	return nil, fmt.Errorf("got a non service object in services store %v", obj)
}

func (kc *kube2consul) handleEndpointAdd(obj interface{}) {
	if e, ok := obj.(*kapi.Endpoints); ok {
		name := bridge.BuildDNSNameString(kc.domain, serviceSubdomain, e.Namespace, e.Name)
		kc.addDNSUsingEndpoints(name, e)
	}
}

func (ks *kube2consul) handlePodCreate(obj interface{}) {
}

func (ks *kube2consul) handlePodUpdate(old interface{}, new interface{}) {
}

func (ks *kube2consul) handlePodDelete(obj interface{}) {
}

func watchEndpoints(kubeClient *kclient.Client, k2c *kube2consul) kcache.Store {
	eStore, eController := kframework.NewInformer(
		bridge.CreateEndpointsLW(kubeClient),
		&kapi.Endpoints{},
		resyncPeriod,
		kframework.ResourceEventHandlerFuncs{
			AddFunc: k2c.handleEndpointAdd,
			UpdateFunc: func(oldObj, newObj interface{}) {
				// TODO: Avoid unwanted updates.
				k2c.handleEndpointAdd(newObj)
			},
		},
	)

	go eController.Run(wait.NeverStop)
	return eStore
}

func watchForServices(kubeClient *kclient.Client, kc *kube2consul) kcache.Store {
	serviceStore, serviceController := kframework.NewInformer(
		bridge.CreateServiceLW(kubeClient),
		&kapi.Service{},
		resyncPeriod,
		kframework.ResourceEventHandlerFuncs{
			AddFunc:    kc.newService,
			DeleteFunc: kc.removeService,
			UpdateFunc: kc.updateService,
		},
	)
	go serviceController.Run(wait.NeverStop)
	return serviceStore
}

func watchPods(kubeClient *kclient.Client, kc *kube2consul) kcache.Store {
	eStore, eController := kframework.NewInformer(
		bridge.CreateEndpointsPodLW(kubeClient),
		&kapi.Pod{},
		resyncPeriod,
		kframework.ResourceEventHandlerFuncs{
			AddFunc: kc.handlePodCreate,
			UpdateFunc: func(oldObj, newObj interface{}) {
				kc.handlePodUpdate(oldObj, newObj)
			},
			DeleteFunc: kc.handlePodDelete,
		},
	)

	go eController.Run(wait.NeverStop)
	return eStore
}

// setupHealthzHandlers sets up a readiness and liveness endpoint for kube2sky.
func setupHealthzHandlers(kc *kube2consul) {
	http.HandleFunc("/readiness", func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprintf(w, "ok\n")
	})
}

func main() {
	flag.CommandLine.SetNormalizeFunc(utilflag.WarnWordSepNormalizeFunc)
	flag.Parse()
	var err error
	setupSignalHandlers()
	// TODO: Validate input flags.
	domain := *argDomain
	if !strings.HasSuffix(domain, ".") {
		domain = fmt.Sprintf("%s.", domain)
	}
	kc := kube2consul{
		domain:              domain,
		etcdMutationTimeout: *argEtcdMutationTimeout,
	}
	if kc.etcdClient, err = bridge.NewEtcdClient(*argEtcdServer); err != nil {
		glog.Fatalf("Failed to create etcd client - %v", err)
	}

	consulClient, err := newConsulClient(*argConsulAgent)
	if err != nil {
		glog.Fatalf("Failed to create Consul client - %v", err)
	}
	kc.consulAgent = consulClient.Agent()

	kubeClient, err := bridge.NewKubeClient(argKubeMasterURL, argKubecfgFile)
	if err != nil {
		glog.Fatalf("Failed to create a kubernetes client: %v", err)
	}
	// Wait synchronously for the Kubernetes service and add a DNS record for it.
	kc.newService(bridge.WaitForKubernetesService(kubeClient))
	glog.Infof("Successfully added DNS record for Kubernetes service.")

	kc.endpointsStore = watchEndpoints(kubeClient, &kc)
	kc.servicesStore = watchForServices(kubeClient, &kc)
	kc.podsStore = watchPods(kubeClient, &kc)

	// We declare kube2sky ready when:
	// 1. It has retrieved the Kubernetes master service from the apiserver. If this
	//    doesn't happen skydns will fail its liveness probe assuming that it can't
	//    perform any cluster local DNS lookups.
	// 2. It has setup the 3 watches above.
	// Once ready this container never flips to not-ready.
	setupHealthzHandlers(&kc)
	glog.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *healthzPort), nil))
}
