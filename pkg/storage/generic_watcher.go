package storage

import (
	"fmt"
	"sync/atomic"
	"time"

	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage/generic"
	utilruntime "k8s.io/kubernetes/pkg/util/runtime"
	"k8s.io/kubernetes/pkg/watch"
)

// Implements watch.Interface
type genericWatcher struct {
	resultChan chan watch.Event
	stopChan   chan struct{}
	stopped    uint32
	raw        generic.InterfaceRawWatch
	storage    *GenericWrapper
	filter     FilterFunc
}

func NewGenericWatcher(raw generic.InterfaceRawWatch, storage *GenericWrapper, filter FilterFunc) *genericWatcher {
	ret := &genericWatcher{
		resultChan: make(chan watch.Event, 100),
		stopChan:   make(chan struct{}),
		raw:        raw,
		storage:    storage,
		filter:     filter,
	}
	go ret.run()
	return ret
}

func (w *genericWatcher) run() {
	defer w.cleanup()
	internalResultChan := w.raw.ResultChan()
	var evIn generic.RawEvent
	for {
		select {
		case <-w.stopChan:
			return

		case evIn = <-internalResultChan:
			var evOut watch.Event
			evOut.Type = evIn.Type
			if evOut.Type == watch.Error {
				evOut.Object = evIn.ErrorStatus.(runtime.Object)
				w.resultChan <- evOut
				return
			} else if evOut.Type == watch.Modified && len(evIn.Current.Data) != 0 && len(evIn.Previous.Data) != 0 {
				objCur, err := w.decodeObject(&evIn.Current)
				if err != nil {
					continue
				}
				objPrev, err := w.decodeObject(&evIn.Previous)
				if err != nil {
					continue
				}
				evOut.Object = objCur
				curFilt := w.filter(objCur)
				prevFilt := w.filter(objPrev)
				switch {
				case prevFilt && !curFilt:
					evOut.Type = watch.Deleted
					evOut.Object = objPrev

				case !prevFilt && curFilt:
					evOut.Type = watch.Added
					evOut.Object = objCur

				case !prevFilt && !curFilt:
					continue
				}
			} else if len(evIn.Current.Data) > 0 {
				obj, err := w.decodeObject(&evIn.Current)
				if err != nil {
					//TODO: glog
					continue
				}
				evOut.Object = obj
			} else if len(evIn.Previous.Data) > 0 {
				obj, err := w.decodeObject(&evIn.Previous)
				if err != nil {
					//TODO: glog
					continue
				}
				evOut.Object = obj
			}
			if evOut.Type != "" {
				select {
				case <-w.stopChan:
					return

				case w.resultChan <- evOut:
				}
			}
		}
	}
}

func (w *genericWatcher) cleanup() {
	close(w.resultChan)
	//close(w.stopChan)
}

func (w *genericWatcher) decodeObject(raw *generic.RawObject) (runtime.Object, error) {
	//if obj, found := w.storage.getFromCache(raw.Version, Everything); found {
	//	return obj, nil
	//}

	obj, err := runtime.Decode(w.storage.Codec(), raw.Data)
	if err != nil {
		return nil, err
	}

	var expiration *time.Time
	if raw.TTL != 0 {
		NewExpiration := time.Now().UTC().Add(time.Duration(raw.TTL) * time.Second)
		expiration = &NewExpiration
	}

	// ensure resource version is set on the object we load from etcd
	if err := w.storage.Versioner().UpdateObject(obj, expiration, raw.Version); err != nil {
		utilruntime.HandleError(fmt.Errorf("failure to version api object (%d) %#v: %v", raw.Version, obj, err))
	}

	// perform any necessary transformation
	//if w.transform != nil {
	//	obj, err = w.transform(obj)
	//	if err != nil {
	//		utilruntime.HandleError(fmt.Errorf("failure to transform api object %#v: %v", obj, err))
	//		return nil, err
	//	}
	//}

	if raw.Version != 0 {
		w.storage.addToCache(raw.Version, obj)
	}
	return obj, nil

}

func (w *genericWatcher) Stop() {
	if atomic.SwapUint32(&w.stopped, 1) == 0 {
		w.raw.Stop()
		close(w.stopChan)
	}
}

func (w *genericWatcher) ResultChan() <-chan watch.Event {
	return w.resultChan
}
