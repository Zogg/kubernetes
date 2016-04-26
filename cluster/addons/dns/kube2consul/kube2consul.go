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
	utilflag "k8s.io/kubernetes/pkg/util/flag"
	"net/url"
)

const (
	// Maximum number of attempts to connect to consul server.
	maxConnectAttempts = 12
	// Resync period for the kube controller loop.
	resyncPeriod = 5 * time.Second
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

type kube2consul struct {
	// TODO: Abstract this so it allows consul or etcd K/V storages.
	// Etcd client.
	etcdClient etcdClient
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

func (kc *kube2consul) newService(obj interface{}) {
	_, err := consulApi.NewClient(consulApi.DefaultConfig())
	if err != nil {
		panic(err)
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

func (kc *kube2consul) handleEndpointAdd(obj interface{}) {
}

func (ks *kube2consul) handlePodCreate(obj interface{}) {
}

func (ks *kube2consul) handlePodUpdate(old interface{}, new interface{}) {
}

func (ks *kube2consul) handlePodDelete(obj interface{}) {
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
