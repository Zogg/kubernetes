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
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/consul"
	"k8s.io/kubernetes/pkg/storage/generic"
	utilnet "k8s.io/kubernetes/pkg/util/net"
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
	return storage.NewGenericWrapperInt(raw, c.Codec, c.Prefix, c.DeserializationCacheSize), nil
}

func newConsulRawStorage(c Config) (generic.InterfaceRaw, error) {
	for _, server := range c.ServerList {
		parsed, err := url.Parse(server)
		if err != nil {
			continue
		}
		clientConfig := c.getConsulApiConfig(parsed)
		client, err := consulapi.NewClient(clientConfig)
		if err != nil {
			continue
		}
		raw := &consul.ConsulKvStorage{
			ConsulKv: *client.KV(),
			// TODO: make this configurable for multiple servers
			ServerList:  []string{clientConfig.Address},
			WaitTimeout: consul.DefaultWaitTimeout,
		}
		return raw, nil
	}
	return nil, fmt.Errorf("No suitable consul server found on any address %v", c.ServerList)
}

func (c *Config) getConsulApiConfig(server *url.URL) *consulapi.Config {
	config := consulapi.DefaultConfig()

	// TODO do stuff to propagate configuration values from our structure
	// to theirs

	if server != nil {
		config.Scheme = server.Scheme
		switch {
		case server.Scheme == "http" || server.Scheme == "https":
			config.Address = server.Host
		case server.Scheme == "unix":
			config.Address = server.String()
		}
	}

	if c.KeyFile != "" && c.CertFile != "" && c.CAFile != "" {
		transport, err := newTransportForConsul(config.Address, c.CertFile, c.KeyFile, c.CAFile)
		if err != nil {
			panic(err)
		}

		config.HttpClient.Transport = transport
	}

	return config
}

func newTransportForConsul(address, certFile, keyFile, caFile string) (*http.Transport, error) {
	tlsConfig := &consulapi.TLSConfig{
		Address:  address,
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   caFile,
	}
	cfg, err := consulapi.SetupTLSConfig(tlsConfig)
	if err != nil {
		return nil, err
	}
	// Copied from etcd.DefaultTransport declaration.
	// TODO: Determine if transport needs optimization
	tr := utilnet.SetTransportDefaults(&http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		MaxIdleConnsPerHost: 500,
		TLSClientConfig:     cfg,
	})
	return tr, nil
}
