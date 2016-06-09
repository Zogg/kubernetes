package testing

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strconv"
	"testing"
	"time"
	
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/etcd"
	"k8s.io/kubernetes/pkg/storage/generic"
	etcdtesting "k8s.io/kubernetes/pkg/storage/etcd/testing"
	"k8s.io/kubernetes/pkg/storage/storagebackend"

	"github.com/golang/glog"
	"golang.org/x/net/context"
)

const ConsulConfig = `
{
  "datacenter": "k8s-testing",
  "log_level": "INFO",
  "node_name": "foobar",
  "server": true,
  "addresses": {
    "https": "127.0.0.1"
  },
  "ports": {
    "https": 8501
  },
  "key_file": "%s",
  "cert_file": "%s",
  "ca_file": "%s"
}
`

func RunTestsForStorageFactories(iterFn func(TestServerFactory) int) {
	factories := GetAllTestStorageFactories()
	retCodes := make([]int, len(factories))
	for idx, factory := range factories {
		fmt.Printf("Running tests in %s mode\n", factory.GetName())
		retCodes[idx] = iterFn(factory)
	}
	for _, code := range retCodes {
		if code > 0 {
			os.Exit(code)
		}
	}
	os.Exit(0)
}

func GetAllTestStorageFactories() []TestServerFactory {
	consulFactory, err := NewConsulTestServerFactory()
	if err != nil {
		panic(fmt.Errorf("unexpected error: %v", err)) // This is a programmer or operator error
		//t.Errorf("unexpected error: %v", err)
	}
	return []TestServerFactory{
		&EtcdTestServerFactory{},
		consulFactory,
	}
}

type TestServerFactory interface {
	NewTestClientServer(t *testing.T) TestServer
	GetName() string
}

type TestServer interface {
	NewRawStorage() generic.InterfaceRaw
	Terminate(t *testing.T)
}

// Etcd implementation
type EtcdTestServerFactory struct {
	
}

func(f *EtcdTestServerFactory) NewTestClientServer(t *testing.T) TestServer {
	return &EtcdTestServer{
		internal:   etcdtesting.NewEtcdTestClientServer(t),
	}
}

func(f *EtcdTestServerFactory) GetName() string {
	return "etcd"
}

type EtcdTestServer struct {
	internal    *etcdtesting.EtcdTestServer
}

func(s *EtcdTestServer) NewRawStorage() generic.InterfaceRaw {
	return etcd.NewEtcdRawStorage(s.internal.Client, false)
}

func(s *EtcdTestServer) Terminate(t *testing.T) {
	s.internal.Terminate(t)
}


// Consul implementation
func NewConsulTestServerFactory() (*ConsulTestServerFactory, error) {
	consul_path := os.Getenv( "CONSUL_EXEC_FILEPATH" )
	if consul_path == "" {
		return nil, errors.New("No path to consul executable found in 'CONSUL_EXEC_FILEPATH'")
	}
	return &ConsulTestServerFactory{
		filePath:   consul_path,
	}, nil
}
type ConsulTestServerFactory struct {
	filePath    string
}

type ConsulTestServer struct {
	cmdServer   *exec.Cmd
	cmdLeave    *exec.Cmd
	config      storagebackend.Config
	CertificatesDir	string
	ConfigFile		string
	Prefix			string
}

type ConsulServerPorts struct {
	dns		int
	http	int
	https	int
	rpc		int
	serf_l	int
	serf_w	int
	server	int
}

var SHARED_CONSUL_SERVER_CONFIG = ConsulServerPorts{
	dns:	-1,
	http:	-1,
	https:	8505,
	rpc:	8506,
	serf_l:	8507,
	serf_w:	8508,
	server:	8509,
}

func consulServerConfigFromPorts(portConfig* ConsulServerPorts) (*ConsulTestServer, error) {
	server := &ConsulTestServer{
		config:     storagebackend.Config{
			Type:	storagebackend.StorageTypeConsul,
			ServerList: 	[]string{"https://127.0.0.1:8501"},
		},
	}
	config := map[string]interface{}{
		"datacenter": "k8s-testing",
		"log_level": "INFO",
		"node_name": "test_svr",
		"server": true,
		"bind_addr": "127.0.0.1",
	}

	addresses := make(map[string]string, 0)
	ports := make(map[string]int, 0)
	
	valid := false
	
	if portConfig.dns > 0 {
		addresses["dns"] = "127.0.0.1"
		ports["dns"] = portConfig.dns
	} else {
		ports["dns"] = -1
	}
	
	if portConfig.http > 0 {
		addresses["http"] = "127.0.0.1"
		ports["http"] = portConfig.http
		server.config.ServerList = []string{ "http://127.0.0.1:" + strconv.Itoa(portConfig.http) }
		valid = true 
	} else {
		ports["http"] = -1
	}
	
	var err error
	
	if portConfig.https > 0 {
		server.CertificatesDir, err = ioutil.TempDir(os.TempDir(), "etcd_certificates")
		if err != nil {
			return nil, err
		}
		server.config.CertFile = path.Join(server.CertificatesDir, "etcdcert.pem")
		if err = ioutil.WriteFile(server.config.CertFile, []byte(etcdtesting.CertFileContent), 0644); err != nil {
			return nil, err
		}
		server.config.KeyFile = path.Join(server.CertificatesDir, "etcdkey.pem")
		if err = ioutil.WriteFile(server.config.KeyFile, []byte(etcdtesting.KeyFileContent), 0644); err != nil {
			return nil, err
		}
		server.config.CAFile = path.Join(server.CertificatesDir, "ca.pem")
		if err = ioutil.WriteFile(server.config.CAFile, []byte(etcdtesting.CAFileContent), 0644); err != nil {
			return nil, err
		}
		config["cert_file"] = server.config.CertFile
		config["key_file"] = server.config.KeyFile
		config["ca_file"] = server.config.CAFile
		config["verify_incoming"] = true
		config["verify_outgoing"] = true
		addresses["https"] = "127.0.0.1"
		ports["https"] = portConfig.https
		server.config.ServerList = []string{ "https://127.0.0.1:" + strconv.Itoa(portConfig.https) }
		valid = true 
	} else {
		ports["https"] = -1
	}
	
	if !valid {
		return nil, errors.New("Invalid consul configuration specified with no client port")
	}
	
	if portConfig.serf_l > 0 {
		ports["serf_lan"] = portConfig.serf_l
	}
	
	if portConfig.serf_w > 0 {
		ports["serf_wan"] = portConfig.serf_w
	}
	
	if portConfig.server > 0 {
		ports["server"] = portConfig.server
	}
	
	if len(addresses) > 0 {
		config["addresses"] = addresses
	}
	if len(ports) > 0 {
		config["ports"] = ports
	}

	server.ConfigFile = path.Join(server.CertificatesDir, "consul.conf")
	configFile, err := os.Create(server.ConfigFile)
	if err != nil {
		return nil, err
	}
	encoder := json.NewEncoder(configFile)
	
	err = encoder.Encode(config)
	if err != nil {
		return nil, err
	}

	return server, nil
}

func(f *ConsulTestServerFactory) NewTestClientServer(t *testing.T) TestServer {
	server, err := consulServerConfigFromPorts(&SHARED_CONSUL_SERVER_CONFIG)
	if err != nil {
		t.Errorf("Unexpected failure starting consul server %#v", err)
	}
	server.cmdLeave = exec.Command(f.filePath, "leave")
	if isUp(&server.config, t) {
		glog.Infof("Consul agent already running... attempting to shut it down")
		exec.Command( f.filePath, "leave" ).Run()
	}
	cmd := exec.Command(f.filePath, "agent", "-dev", "-config-file", server.ConfigFile)
	err = cmd.Start()
	if err != nil {
		t.Errorf("unexpected error while starting consul: %v", err)
	}
	
	server.cmdServer = cmd

	err = server.waitUntilUp(t)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	return server
}

func(f *ConsulTestServerFactory) GetName() string {
	return "consul"
}

func refModify(config *storagebackend.Config, key string, countDelta int, createIfNotFound bool) (newCount int, modifyIndex uint64, err error) {
	rawStorage, err := storagebackend.CreateRaw(*config)
	if err != nil {
		return 0, 0, err
	}
	for {
		var rawObj generic.RawObject
		err := rawStorage.Get(context.TODO(), key, &rawObj)
		var refCount int
		if err != nil {
			if storage.IsNotFound(err) && createIfNotFound {
				refCount = 0
			} else {
				return 0, 0, err
			}
		} else {
			refCount, err = strconv.Atoi(string(rawObj.Data))
		}
		refCount += countDelta
		if refCount != 0 {
			rawObj.Data = []byte(strconv.Itoa(refCount))
			if rawObj.Version != 0 {
				success, err := rawStorage.Set(context.TODO(), key, &rawObj)
				if success {
					return refCount, rawObj.Version, nil
				}
				if err != nil {
					return 0, 0, err
				}
			} else {
				err = rawStorage.Create(context.TODO(), key, rawObj.Data, &rawObj, 0)
				if err != nil {
					if !storage.IsNodeExist(err) {
						return 0, 0, err
					}
				} else {
					// we have successfully created a refCount
					return refCount, rawObj.Version, nil
				}
			}
		} else {
			if rawObj.Version == 0 {
				// we have successfully done nothing to nothing, but why?
				return 0, 0, nil
			}
			precondition := func(rawFilt *generic.RawObject) (bool, error) {
				return rawFilt.Version == rawObj.Version, nil
			}
			err = rawStorage.Delete(context.TODO(), key, nil, precondition)
			if err != nil {
				if !storage.IsTestFailed(err) {
					return 0, 0, err
				}
			} else {
				// we have successfully deleted the ref key at count 0
				return 0, 0, nil
			}
		}
	}
}

func isUp(config *storagebackend.Config, t *testing.T) bool {
	rawStorage, err := storagebackend.CreateRaw(*config)
	if err != nil {
		glog.Infof("Failed to get raw storage (retrying): %v", err)
		return false
	}
	var rawObj generic.RawObject
	err = rawStorage.Get(context.TODO(), "until/consul/started", &rawObj )
	if err == nil || storage.IsNotFound(err) {
		return true
	}
	return false
}

// waitForEtcd wait until consul is propagated correctly
func (s *ConsulTestServer) waitUntilUp(t *testing.T) error {
	for start := time.Now(); time.Since(start) < 25*time.Second; time.Sleep(100 * time.Millisecond) {
		if isUp(&s.config, t) {
			return nil
		}
	}
	return fmt.Errorf("timeout on waiting for consul cluster")
}


func(s *ConsulTestServer) NewRawStorage() generic.InterfaceRaw {
	ret, _ := storagebackend.CreateRaw(s.config)
	return ret
}

func(s *ConsulTestServer) Terminate(t *testing.T) {
	err := s.cmdLeave.Run()
	if err != nil {
		// well damn... what do we do now?
		t.Errorf("unexpected error while stopping consul: %v", err)
	}
	s.cmdServer.Wait()
	if err := os.RemoveAll(s.CertificatesDir); err != nil {
		t.Fatal(err)
	}
}
