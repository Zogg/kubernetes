package etcd3

import (
	"k8s.io/kubernetes/pkg/storage/generic"

	"fmt"
	etcd "github.com/coreos/etcd/client"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	utilruntime "k8s.io/kubernetes/pkg/util/runtime"
	"k8s.io/kubernetes/pkg/watch"
)

// etcdWatcher converts a native etcd watch to a watch.Interface.
type etcd3WatcherRaw struct {
	outgoing chan generic.RawEvent
	emit     func(generic.RawEvent)
}

// newEtcdWatcher returns a new etcdWatcher; if list is true, watch sub-nodes.
// The versioner must be able to handle the objects that transform creates.

func newEtcd3WatcherRaw(list bool, quorum bool) *etcd3WatcherRaw {
	w := &etcd3WatcherRaw{
		outgoing: make(chan generic.RawEvent),
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
	if res.Node == nil {
		utilruntime.HandleError(fmt.Errorf("unexpected nil node: %#v", res))
		return
	}
	action := watch.Added
	if res.Node.ModifiedIndex != res.Node.CreatedIndex {
		action = watch.Modified
	}
	ev := generic.RawEvent{
		Type: action,
	}
	copyNode(res.Node, &ev.Current)
	w.emit(ev)
}

func (w *etcd3WatcherRaw) sendModify(res *etcd.Response) {
	if res.Node == nil {
		glog.Errorf("unexpected nil node: %#v", res)
		return
	}
	if w.include != nil && !w.include(res.Node.Key) {
		return
	}
	ev := generic.RawEvent{
		Type: watch.Modified,
	}
	copyNode(res.Node, &ev.Current)
	copyNode(res.PrevNode, &ev.Previous)
	w.emit(ev)

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
