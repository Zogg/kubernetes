package etcd3

import (
	"k8s.io/kubernetes/pkg/storage/generic"

	etcd "github.com/coreos/etcd/client"

	"golang.org/x/net/context"
)

// etcdWatcher converts a native etcd watch to a watch.Interface.
type etcd3WatcherRaw struct {
	outgoing      chan generic.RawEvent
}

// newEtcdWatcher returns a new etcdWatcher; if list is true, watch sub-nodes.
// The versioner must be able to handle the objects that transform creates.

func newEtcd3WatcherRaw(list bool, quorum bool/*, include etcd.includeFunc*/) *etcd3WatcherRaw {
	w := &etcd3WatcherRaw{
		outgoing:     make(chan generic.RawEvent),
	}
	return w
}

// etcdWatch calls etcd's Watch function, and handles any errors. Meant to be called
// as a goroutine.
func (w *etcd3WatcherRaw) etcdWatch(ctx context.Context, client etcd.KeysAPI, key string, resourceVersion uint64) {
}

// translate pulls stuff from etcd, converts, and pushes out the outgoing channel. Meant to be
// called as a goroutine.
func (w *etcd3WatcherRaw) translate() {

}

func (w *etcd3WatcherRaw) sendAdd(res *etcd.Response) {

}

func (w *etcd3WatcherRaw) sendModify(res *etcd.Response) {

}

func (w *etcd3WatcherRaw) sendDelete(res *etcd.Response) {

}

func (w *etcd3WatcherRaw) sendResult(res *etcd.Response) {

}

// ResultChan implements watch.Interface.
func (w *etcd3WatcherRaw) ResultChan() <-chan generic.RawEvent {
	return w.outgoing
}

// Stop implements watch.Interface.
func (w *etcd3WatcherRaw) Stop() {

}
