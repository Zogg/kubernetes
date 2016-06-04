package testing

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
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
    "https": "0.0.0.0"
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

func(f *ConsulTestServerFactory) NewTestClientServer(t *testing.T) TestServer {
	server := &ConsulTestServer{
		config:     storagebackend.Config{
			Type:	storagebackend.StorageTypeConsul,
			ServerList: 	[]string{"https://127.0.0.1:8501"},
		},
	}
	var err error
	server.CertificatesDir, err = ioutil.TempDir(os.TempDir(), "etcd_certificates")
	if err != nil {
		t.Fatal(err)
	}
	server.config.CertFile = path.Join(server.CertificatesDir, "etcdcert.pem")
	if err = ioutil.WriteFile(server.config.CertFile, []byte(etcdtesting.CertFileContent), 0644); err != nil {
		t.Fatal(err)
	}
	server.config.KeyFile = path.Join(server.CertificatesDir, "etcdkey.pem")
	if err = ioutil.WriteFile(server.config.KeyFile, []byte(etcdtesting.KeyFileContent), 0644); err != nil {
		t.Fatal(err)
	}
	server.config.CAFile = path.Join(server.CertificatesDir, "ca.pem")
	if err = ioutil.WriteFile(server.config.CAFile, []byte(etcdtesting.CAFileContent), 0644); err != nil {
		t.Fatal(err)
	}

	server.ConfigFile = path.Join(server.CertificatesDir, "consul.conf")
	if err = ioutil.WriteFile(server.ConfigFile, []byte(fmt.Sprintf(ConsulConfig, server.config.KeyFile, server.config.CertFile, server.config.CAFile)), 0644); err != nil {
		t.Fatal(err)
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

type ConsulTestServer struct {
	cmdServer   *exec.Cmd
	cmdLeave    *exec.Cmd
	config      storagebackend.Config
	CertificatesDir	string
	ConfigFile		string
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
