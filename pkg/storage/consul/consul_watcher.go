package consul 

import (
	//"fmt"
	//"net/http"
	//"errors"
	//"strings"
	"sort"
	"sync"
	//"sync/atomic"
	//"time"
  
	//"k8s.io/kubernetes/pkg/conversion"
	"k8s.io/kubernetes/pkg/runtime"
	//"k8s.io/kubernetes/pkg/storage"
	// TODO: relocate APIObjectVersioner to storage.APIObjectVersioner_uint64
	//"k8s.io/kubernetes/pkg/storage/etcd" // for the purpose of APIObjectVersioner
	"k8s.io/kubernetes/pkg/watch"

	consulapi "github.com/hashicorp/consul/api"
)

type retainedValue struct {
	value       []byte
	version     uint64
}

type consulWatch struct {
	stopChan    chan bool
	resultChan  chan watch.Event
	storage     *ConsulKvStorage
	stopLock    sync.Mutex
	stopped     bool
	emit        func(watch.Event) bool
}

func nullEmitter(w watch.Event) bool {
	return false
}

func(s *ConsulKvStorage) newConsulWatch(key string, version uint64, deep bool) (*consulWatch, error) {
	if deep {
		KVs, qm, err := s.ConsulKv.List(key, nil)
		if err != nil {
			return nil, err
		}
		w := &consulWatch{
			stopChan:   make( chan bool, 1 ),
			resultChan: make( chan watch.Event ),
			storage:    s,
			stopped:    false,
		}
		w.emit = func(ev watch.Event) bool {
			select {
				case <-w.stopChan:
					return false
					
				case w.resultChan <- ev:
					return true 
			}
		}
		if version == 0 {
			version = qm.LastIndex
		}
		go w.watchDeep(key, version, KVs)
		return w, nil
	} else {
		kv, qm, err := s.ConsulKv.Get(key, nil)
		if err != nil {
			return nil, err
		}
		w := &consulWatch{
			stopChan:   make( chan bool, 1 ),
			resultChan: make( chan watch.Event ),
			storage:    s,
			stopped:    false,
		}
		w.emit = func(ev watch.Event) bool {
			select {
				case <-w.stopChan:
					return false
					
				case w.resultChan <- ev:
					return true 
			}
		}
		if version == 0 {
			if kv != nil {
				version = kv.ModifyIndex
			} else if qm != nil {
				version = qm.LastIndex
			}
		}
		go w.watchSingle(key, version, kv)
		return w, nil
	}
}

type ByKey []*consulapi.KVPair
func(kvs ByKey) Len() int           { return len(kvs) }
func(kvs ByKey) Swap(i, j int)      { kvs[i], kvs[j] = kvs[j], kvs[i] }

// *** assume that nil entries will NEVER be produced by consul's client
//func(kvs ByKey) Less(i, j int)  { (kvs[i] == nil && kvs[j] != nil) || (kvs[j] != nil && kvs[i].Key < kvs[j].Key) }
func(kvs ByKey) Less(i, j int) bool { return kvs[i].Key < kvs[j].Key }

func(w *consulWatch) watchDeep(key string, version uint64, kvsLast []*consulapi.KVPair) {
	defer w.clean()
	cont := true
	sort.Sort(ByKey(kvsLast))
	kvs := kvsLast
	versionNext := version
	for cont {
		j := 0
		for _, kv := range kvs {
			for ; j < len(kvsLast) && kvsLast[j].Key < kv.Key; j++ {
				cont = w.emitEvent( watch.Deleted, kvsLast[j] )
			}

			kvLast := kvsLast[j]
			
			if kv.Key != kvLast.Key {
				cont = w.emitEvent( watch.Added, kv )
			} else if kv.ModifyIndex > version {
				cont = w.emitEvent( watch.Modified, kv )
			}
					
			if !cont {
				return
			}
		}
		
		kvsLast = kvs
		version = versionNext
		kvs, qm, err := w.storage.ConsulKv.List( key, &consulapi.QueryOptions{ WaitIndex: version, WaitTime: w.storage.Config.Config.WaitTimeout } )
		if err != nil {
			w.emitError( key, err )
			return
		}
		sort.Sort(ByKey(kvs))
		versionNext = qm.LastIndex
	}
}


func(w *consulWatch) watchSingle(key string, version uint64, kvLast *consulapi.KVPair) {
	defer w.clean()
	cont := true
	kv := kvLast
	versionNext := version
	for cont {
		if kv == nil && kvLast != nil {
			cont = w.emitEvent( watch.Deleted, kvLast )
		}
		if kv != nil {
			if kv.ModifyIndex > version {
				if kv.CreateIndex > version {
					cont = w.emitEvent( watch.Added, kv )
				} else {
					cont = w.emitEvent( watch.Modified, kv )
				}
			}
		}
		
		if !cont {
			return
		}
		
		kvLast = kv
		version = versionNext
		var qm *consulapi.QueryMeta
		var err error
		kv, qm, err = w.storage.ConsulKv.Get( key, &consulapi.QueryOptions{ WaitIndex: version, WaitTime: w.storage.Config.Config.WaitTimeout } )
		if err != nil {
			w.emitError( key, err )
			return
		}
		versionNext = qm.LastIndex
	}
}

func(w *consulWatch) clean() {
	close(w.stopChan)
	close(w.resultChan)
}

func(w *consulWatch) emitEvent( action watch.EventType, kv *consulapi.KVPair ) bool {
	if kv != nil {
		obj, err := runtime.Decode(w.storage.codec, kv.Value)
		if err != nil {
			return !w.stopped
		}
		return w.emit( watch.Event{ Type: action, Object: obj } )
	}
	return !w.stopped
}

func(w *consulWatch) emitError( key string, err error ) {
	
}

func(w *consulWatch) Stop() {
	w.emit = nullEmitter
	if w.stopped {
		return
	}
	w.stopLock.Lock()
	defer w.stopLock.Unlock()
	if w.stopped {
		return
	}
	w.stopChan <- true
}

func(w *consulWatch) ResultChan() <-chan watch.Event {
	return w.resultChan
}
