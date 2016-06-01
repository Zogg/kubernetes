package etcd3

import (
	"k8s.io/kubernetes/pkg/storage/generic"

	"fmt"
	"github.com/coreos/etcd/clientv3"
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
	defer close(w.outgoing)
	defer utilruntime.HandleCrash()

	for {
		select {
		case err := <-w.etcdError:
			if err != nil {
				var status *unversioned.Status
				switch {
				case etcdutil.IsEtcdWatchExpired(err):
					status = &unversioned.Status{
						Status:  unversioned.StatusFailure,
						Message: err.Error(),
						Code:    http.StatusGone, // Gone
						Reason:  unversioned.StatusReasonExpired,
					}
				// TODO: need to generate errors using api/errors which has a circular dependency on this package
				//   no other way to inject errors
				// case etcdutil.IsEtcdUnreachable(err):
				//   status = errors.NewServerTimeout(...)
				default:
					status = &unversioned.Status{
						Status:  unversioned.StatusFailure,
						Message: err.Error(),
						Code:    http.StatusInternalServerError,
						Reason:  unversioned.StatusReasonInternalError,
					}
				}
				w.emit(generic.RawEvent{
					Type:        watch.Error,
					ErrorStatus: status,
				})
			}
			return
		case <-w.userStop:
			return
		case res, ok := <-w.etcdIncoming:
			if ok {
				if curLen := int64(len(w.etcdIncoming)); watchChannelHWM.Update(curLen) {
					// Monitor if this gets backed up, and how much.
					glog.V(2).Infof("watch: %v objects queued in channel.", curLen)
				}
				w.sendResult(res)
			}
			// If !ok, don't return here-- must wait for etcdError channel
			// to give an error or be closed.
		}
	}
}

func (w *etcd3WatcherRaw) sendResult(res *clientv3.WatchResponse) {
	switch res.Action {
	case EtcdCreate, EtcdGet:
		w.sendAdd(res)
	case EtcdSet, EtcdCAS:
		w.sendModify(res)
	case EtcdDelete, EtcdExpire, EtcdCAD:
		w.sendDelete(res)
	default:
		utilruntime.HandleError(fmt.Errorf("unknown action: %v", res.Action))
	}
}

func (w *etcd3WatcherRaw) sendAdd(res *clientv3.WatchResponse) {
	if res.Node == nil {
		utilruntime.HandleError(fmt.Errorf("unexpected nil node: %#v", res))
		return
	}
	if w.include != nil && !w.include(res.Node.Key) {
		return
	}
	action := watch.Added
	if res.Node.ModifiedIndex != res.Node.CreatedIndex {
		action = watch.Modified
	}
	ev := generic.RawEvent{
		Type: action,
	}
	//copyNode(res.Node, &ev.Current)
	w.emit(ev)
}

func (w *etcd3WatcherRaw) sendModify(res *clientv3.WatchResponse) {
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
	//copyNode(res.Node, &ev.Current)
	//copyNode(res.PrevNode, &ev.Previous)
	w.emit(ev)
}

func (w *etcd3WatcherRaw) sendDelete(res *clientv3.WatchResponse) {
	if res.PrevNode == nil {
		utilruntime.HandleError(fmt.Errorf("unexpected nil prev node: %#v", res))
		return
	}
	if w.include != nil && !w.include(res.PrevNode.Key) {
		return
	}
	node := *res.PrevNode
	if res.Node != nil {
		// Note that this sends the *old* object with the etcd index for the time at
		// which it gets deleted. This will allow users to restart the watch at the right
		// index.
		node.ModifiedIndex = res.Node.ModifiedIndex
	}
	ev := generic.RawEvent{
		Type: watch.Deleted,
	}
	//copyNode(&node, &ev.Previous)
	w.emit(ev)
}

// ResultChan implements watch.Interface.
func (w *etcd3WatcherRaw) ResultChan() <-chan generic.RawEvent {
	return w.outgoing
}

// Stop implements watch.Interface.
func (w *etcd3WatcherRaw) Stop() {

}
