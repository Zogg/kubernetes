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
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	etcd "github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
	consulApi "github.com/hashicorp/consul/api"
	skymsg "github.com/skynetservices/skydns/msg"
	flag "github.com/spf13/pflag"
	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/endpoints"
	"k8s.io/kubernetes/pkg/api/unversioned"
	kcache "k8s.io/kubernetes/pkg/client/cache"
	"k8s.io/kubernetes/pkg/client/restclient"
	kclient "k8s.io/kubernetes/pkg/client/unversioned"
	kclientcmd "k8s.io/kubernetes/pkg/client/unversioned/clientcmd"
	kframework "k8s.io/kubernetes/pkg/controller/framework"
	kselector "k8s.io/kubernetes/pkg/fields"
	etcdutil "k8s.io/kubernetes/pkg/storage/etcd/util"
	utilflag "k8s.io/kubernetes/pkg/util/flag"
	"k8s.io/kubernetes/pkg/util/validation"
	"k8s.io/kubernetes/pkg/util/wait"
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
	if kc.etcdClient, err = newEtcdClient(*argEtcdServer); err != nil {
		glog.Fatalf("Failed to create etcd client - %v", err)
	}

	kubeClient, err := newKubeClient()
	if err != nil {
		glog.Fatalf("Failed to create a kubernetes client: %v", err)
	}
	// Wait synchronously for the Kubernetes service and add a DNS record for it.
	kc.newService(waitForKubernetesService(kubeClient))
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
