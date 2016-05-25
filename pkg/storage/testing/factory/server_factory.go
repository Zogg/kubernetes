package testing

import (
	"errors"
	"fmt"
	//"io"
	"os"
	"os/exec"
	"testing"
	"time"
	
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/consul"
	"k8s.io/kubernetes/pkg/storage/etcd"
	"k8s.io/kubernetes/pkg/storage/generic"
	etcdtesting "k8s.io/kubernetes/pkg/storage/etcd/testing"

	"github.com/golang/glog"
	"golang.org/x/net/context"
)

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
	if isUp(&consul.ConsulConfig{}, t) {
		glog.Infof("Consul agent already running... attempting to shut it down")
		exec.Command( f.filePath, "leave" ).Run()
	}
	cmd := exec.Command( f.filePath, "agent", "-dev" )
	err := cmd.Start()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	
	server := &ConsulTestServer{
		cmdServer:  cmd,
		cmdLeave:   exec.Command( f.filePath, "leave" ),
	}
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
	config      consul.ConsulConfig
}

func isUp(config *consul.ConsulConfig, t *testing.T) bool {
	rawStorage, err := config.NewRawStorage()
	if err != nil {
		t.Logf("Failed to get raw storage (retrying): %v", err)
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
	t.Errorf("timeout on waiting for consul cluster")
	return fmt.Errorf("timeout on waiting for consul cluster")
}


func(s *ConsulTestServer) NewRawStorage() generic.InterfaceRaw {
	ret, _ := s.config.NewRawStorage()
	return ret
}

func(s *ConsulTestServer) Terminate(t *testing.T) {
	fmt.Println("Shutting down consul agent")
	err := s.cmdLeave.Run()
	if err != nil {
		// well damn... what do we do now?
		t.Errorf("unexpected error: %v", err)
	}
	s.cmdServer.Wait()
}
