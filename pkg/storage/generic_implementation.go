package storage

import (
	"bytes"
	"errors"
	"fmt"
	"path"
	"reflect"
	"strconv"
	"strings"
	"time"
	
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/meta"
	"k8s.io/kubernetes/pkg/conversion"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage/generic"
	"k8s.io/kubernetes/pkg/util"
	utilcache "k8s.io/kubernetes/pkg/util/cache"
	utilruntime "k8s.io/kubernetes/pkg/util/runtime"
	"k8s.io/kubernetes/pkg/watch"

	"github.com/golang/glog"
	"golang.org/x/net/context"
)

type genericWrapper struct {
	generic     generic.InterfaceRaw
	versioner   Versioner
	codec       runtime.Codec
	copier      runtime.ObjectCopier
	pathPrefix  string
	cache utilcache.Cache
}


func NewGenericWrapper(raw generic.InterfaceRaw, codec runtime.Codec, prefix string, deserializationCacheSize int) Interface {
	return &genericWrapper{
		generic:    raw,
		versioner:  APIObjectVersioner{},
		codec:      codec,
		copier:     api.Scheme,
		pathPrefix: path.Join("/", prefix),
		cache:      utilcache.NewCache(deserializationCacheSize),
	}
} 


func(s *genericWrapper) Backends(ctx context.Context) []string {
	return s.generic.Backends(ctx)
}

func(s *genericWrapper) Versioner() Versioner {
	return s.versioner
}

func(s *genericWrapper) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	trace := util.NewTrace("GenericWrapper::Create " + getTypeName(obj))
	defer trace.LogIfLong(250 * time.Millisecond)
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	key = s.prefixKey(key)
	data, err := runtime.Encode(s.codec, obj)
	trace.Step("Object encoded")
	if err != nil {
		return err
	}
	if version, err := s.versioner.ObjectResourceVersion(obj); err == nil && version != 0 {
		return errors.New("resourceVersion may not be set on objects to be created")
	}
	trace.Step("Version checked")
	
	raw_out := generic.RawObject{} 
	err = s.generic.Create(ctx, key, data, &raw_out, ttl)
	if err != nil {
		return err
	}
	if out != nil {
		if _, err := conversion.EnforcePtr(out); err != nil {
			panic("unable to convert output object to pointer")
		}
		err = s.extractObj(raw_out, err, out, false)
	}
	return err
}

func(s *genericWrapper) Delete(ctx context.Context, key string, out runtime.Object, preconditions *Preconditions) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	key = s.prefixKey(key)
	v, err := conversion.EnforcePtr(out)
	if err != nil {
		panic("unable to convert output object to pointer")
	}
	obj := reflect.New(v.Type()).Interface().(runtime.Object)
	
	var filter generic.RawFilterFunc
	if preconditions != nil {
		filter = func(raw *generic.RawObject) (bool, error) {
			err = s.extractObj(*raw, nil, obj, false)
			if err != nil {
				return false, err
			}
			if err := checkPreconditions(key, 0, preconditions, obj); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	raw_out := generic.RawObject{} 
	err = s.generic.Delete(ctx, key, &raw_out, filter)
	if err != nil {
		return err
	}
	err = s.extractObj(raw_out, err, out, false)
	return err
}

func(s *genericWrapper) Watch(ctx context.Context, key string, resourceVersion string, filter FilterFunc) (watch.Interface, error) {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	key = s.prefixKey(key)
	raw, err := s.generic.Watch(ctx, key, resourceVersion)
	if err != nil {
		return nil, err
	}
	if filter == nil {
		filter = func(obj runtime.Object) bool { return true }
	}
	return newGenericWatcher(raw, s, filter, key), nil
}

func(s *genericWrapper) WatchList(ctx context.Context, key string, resourceVersion string, filter FilterFunc) (watch.Interface, error) {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	key = s.prefixKey(key)
	raw, err := s.generic.WatchList(ctx, key, resourceVersion)
	if err != nil {
		return nil, err
	}
	return newGenericWatcher(raw, s, filter, key), nil
}


func(s *genericWrapper) Get(ctx context.Context, key string, objPtr runtime.Object, ignoreNotFound bool) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	key = s.prefixKey(key)
	var raw generic.RawObject
	err := s.generic.Get(ctx, key, &raw)
	if err != nil {
		return err
	}
	err = s.extractObj(raw, err, objPtr, ignoreNotFound)
	return err
}

func(s *genericWrapper) GetToList(ctx context.Context, key string, filter FilterFunc, listObj runtime.Object) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	key = s.prefixKey(key)
	rawList := make([]generic.RawObject,0)
	listVersion, err := s.generic.GetToList(ctx, key, &rawList)
	if err != nil && !IsNotFound(err) {
		return err
	}
	return s.outputList(key, filter, listObj, listVersion, rawList)
}

func(s *genericWrapper) List(ctx context.Context, key string, resourceVersion string, filter FilterFunc, listObj runtime.Object) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	key = s.prefixKey(key)
	rawList := make([]generic.RawObject,0)
	listVersion, err := s.generic.List(ctx, key, resourceVersion, &rawList)
	if err != nil {
		return err
	}
	return s.outputList(key, filter, listObj, listVersion, rawList)
}

func(s *genericWrapper) outputList(key string, filter FilterFunc, listObj runtime.Object, listVersion uint64, rawList []generic.RawObject) error {
	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		// This should not happen at runtime.
		panic("need ptr to slice")
	}
	var maxIndex uint64
	for _, raw := range rawList {
		if obj, found := s.getFromCache(raw.Version, filter); found {
			// obj != nil iff it matches the filter function.
			if obj != nil {
				v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
			}
		} else {
			obj, _, err := s.codec.Decode(raw.Data, nil, reflect.New(v.Type().Elem()).Interface().(runtime.Object))
			if err != nil {
				return err
			}
	
			// being unable to set the version does not prevent the object from being extracted
			_ = s.versioner.UpdateObject(obj, raw.Version)
			if filter(obj) {
				v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
			}
			if raw.Version != 0 {
				s.addToCache(raw.Version, obj)
			}
		}
		if maxIndex < raw.Version {
			maxIndex = raw.Version
		}
	}
	if listVersion != 0 {
		maxIndex = listVersion
	}
	if err := s.versioner.UpdateList(listObj, maxIndex); err != nil {
		return err
	}
	return nil
}

func(s *genericWrapper) GuaranteedUpdate(ctx context.Context, key string, ptrToType runtime.Object, ignoreNotFound bool, preconditions *Preconditions, tryUpdate UpdateFunc) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	v, err := conversion.EnforcePtr(ptrToType)
	if err != nil {
		// Panic is appropriate, because this is a programming error.
		panic("need ptr to type")
	}
	key = s.prefixKey(key)
	for {
		obj := reflect.New(v.Type()).Interface().(runtime.Object)
		raw := generic.RawObject{}
		if err := s.generic.Get(ctx, key, &raw); err != nil && (!ignoreNotFound || !IsNotFound(err)) {
			return err
		}
		if err = s.extractObj(raw, err, obj, ignoreNotFound); err != nil {
			return err
		}
		if err := checkPreconditions(key, raw.Version, preconditions, obj); err != nil {
			return err
		}
		meta := ResponseMeta{
			TTL:                raw.TTL,
			ResourceVersion:    raw.Version,
		}
		outObj, newTTL, err := tryUpdate(obj, meta)
		if err != nil {
			return err
		}
		if newTTL != nil {
			raw.TTL = int64(*newTTL)
		}
		if err := s.versioner.UpdateObject(outObj, 0); err != nil {
			return errors.New("resourceVersion cannot be set on objects store in kv")
		}
		data, err := runtime.Encode(s.codec, outObj)
		if err != nil {
			return err
		}
		if bytes.Compare(raw.Data, data) == 0 {
			return s.extractObj(raw, nil, ptrToType, ignoreNotFound)
		}
		raw.Data = data
		succeeded, err := s.generic.Set(ctx, key, &raw)
		if err != nil {
			if IsNodeExist(err) || IsTestFailed(err) {
				continue
			}
			if !ignoreNotFound || !IsNotFound(err) {
				return err
			}
		}
		if succeeded {
			return s.extractObj(raw, nil, ptrToType, ignoreNotFound)
		}
	}
}

func(s *genericWrapper) Codec() runtime.Codec {
	return s.codec
}

func (s *genericWrapper) extractObj(raw generic.RawObject, inErr error, objPtr runtime.Object, ignoreNotFound bool) error {
	if inErr != nil || len(raw.Data) == 0 {
		if ignoreNotFound {
			v, err := conversion.EnforcePtr(objPtr)
			if err != nil {
				return err
			}
			v.Set(reflect.Zero(v.Type()))
			return nil
		} else if inErr != nil {
			return inErr
		}
		return fmt.Errorf("unable to locate a value on the RawObject: %#v", raw)
	}
	out, gvk, err := s.codec.Decode([]byte(raw.Data), nil, objPtr)
	if err != nil {
		return err
	}
	if out != objPtr {
		return fmt.Errorf("unable to decode object %s into %v", gvk.String(), reflect.TypeOf(objPtr))
	}
	// being unable to set the version does not prevent the object from being extracted
	_ = s.versioner.UpdateObject(objPtr, raw.Version)
	return err
}

func (s *genericWrapper) prefixKey(key string) string {
	if strings.HasPrefix(key, s.pathPrefix) {
		return key
	}
	return path.Join(s.pathPrefix, key)
}

func (h *genericWrapper) getFromCache(index uint64, filter FilterFunc) (runtime.Object, bool) {
	//startTime := time.Now()
	//defer func() {
	//	metrics.ObserveGetCache(startTime)
	//}()
	obj, found := h.cache.Get(index)
	if found {
		if !filter(obj.(runtime.Object)) {
			return nil, true
		}
		// We should not return the object itself to avoid polluting the cache if someone
		// modifies returned values.
		objCopy, err := h.copier.Copy(obj.(runtime.Object))
		if err != nil {
			glog.Errorf("Error during DeepCopy of cached object: %q", err)
			// We can't return a copy, thus we report the object as not found.
			return nil, false
		}
		//metrics.ObserveCacheHit()
		return objCopy.(runtime.Object), true
	}
	//metrics.ObserveCacheMiss()
	return nil, false
}

func (h *genericWrapper) addToCache(index uint64, obj runtime.Object) {
	//startTime := time.Now()
	//defer func() {
	//	metrics.ObserveAddCache(startTime)
	//}()
	objCopy, err := h.copier.Copy(obj)
	if err != nil {
		glog.Errorf("Error during DeepCopy of cached object: %q", err)
		return
	}
	h.cache.Add(index, objCopy)
	//isOverwrite := h.cache.Add(index, objCopy)
	//if !isOverwrite {
	//	metrics.ObserveNewEntry()
	//}
}

type genericWatcher struct {
	resultChan  chan watch.Event
	stopped     bool
	raw         generic.InterfaceRawWatch
	storage     *genericWrapper
	filter      FilterFunc
	name        string
}

func newGenericWatcher(raw generic.InterfaceRawWatch, storage *genericWrapper, filter FilterFunc, name string) *genericWatcher {
	ret := &genericWatcher{
		resultChan: make(chan watch.Event, 100),
		raw:        raw,
		storage:    storage,
		filter:     filter,
		name:       name,
	}
	go ret.run()
	return ret
}

func(w *genericWatcher) run() {
	defer w.cleanup()
	internalResultChan := w.raw.ResultChan()
	for evIn := range internalResultChan {
		var evOut watch.Event
		evOut.Type = evIn.Type
		if evOut.Type == watch.Error {
			evOut.Object = evIn.ErrorStatus.(runtime.Object)
			w.resultChan<-evOut
			return
		} else {
			var curFilt, prevFilt bool
			if len(evIn.Previous.Data) > 0 {
				obj, err := w.decodeObject(&evIn.Previous)
				if err != nil {
					glog.Infof("Watcher-cooked %s: error ignore", w.name)
					continue
				}
				prevFilt = w.filter(obj)
				if prevFilt {
					evOut.Object = obj
				}
			}
			if len(evIn.Current.Data) > 0 {
				obj, err := w.decodeObject(&evIn.Current)
				if err != nil {
					glog.Infof("Watcher-cooked %s: error ignore", w.name)
					continue
				}
				curFilt = w.filter(obj)
				if curFilt {
					evOut.Object = obj
				}
			}
			switch {
				case prevFilt && !curFilt:
					evOut.Type = watch.Deleted
				
				case !prevFilt && curFilt:
					evOut.Type = watch.Added
				
				case !prevFilt && !curFilt:
					continue
					
				default:
					evOut.Type = watch.Modified
			}
		}
		if evOut.Type != "" { 
			select {
				case <-time.After(30 * time.Second):
					glog.Errorf("Cooked watcher left with dangling output while watching %s", w.name)
					return
			
				case w.resultChan<-evOut:
					if evOut.Type == watch.Error {
						return
					}
			}
		}
	}
}

func(w *genericWatcher) cleanup() {
	close(w.resultChan)
}

func (w *genericWatcher) decodeObject(raw *generic.RawObject) (runtime.Object, error) {
	if obj, found := w.storage.getFromCache(raw.Version, Everything); found {
		return obj, nil
	}

	obj, err := runtime.Decode(w.storage.codec, raw.Data)
	if err != nil {
		return nil, err
	}

	// ensure resource version is set on the object we load from etcd
	if err := w.storage.versioner.UpdateObject(obj, raw.Version); err != nil {
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

func(w *genericWatcher) Stop() {
	w.stopped = true
	w.raw.Stop()
}

func(w *genericWatcher) ResultChan() <-chan watch.Event {
	return w.resultChan
}

type APIObjectVersioner struct{}

// UpdateObject implements Versioner
func (a APIObjectVersioner) UpdateObject(obj runtime.Object, resourceVersion uint64) error {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	versionString := ""
	if resourceVersion != 0 {
		versionString = strconv.FormatUint(resourceVersion, 10)
	}
	accessor.SetResourceVersion(versionString)
	return nil
}

// UpdateList implements Versioner
func (a APIObjectVersioner) UpdateList(obj runtime.Object, resourceVersion uint64) error {
	listMeta, err := api.ListMetaFor(obj)
	if err != nil || listMeta == nil {
		return err
	}
	versionString := ""
	if resourceVersion != 0 {
		versionString = strconv.FormatUint(resourceVersion, 10)
	}
	listMeta.ResourceVersion = versionString
	return nil
}

// ObjectResourceVersion implements Versioner
func (a APIObjectVersioner) ObjectResourceVersion(obj runtime.Object) (uint64, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return 0, err
	}
	version := accessor.GetResourceVersion()
	if len(version) == 0 {
		return 0, nil
	}
	return strconv.ParseUint(version, 10, 64)
}

func getTypeName(obj interface{}) string {
	return reflect.TypeOf(obj).String()
}

func checkPreconditions(key string, rv uint64, preconditions *Preconditions, out runtime.Object) error {
	if preconditions == nil {
		return nil
	}
	objMeta, err := api.ObjectMetaFor(out)
	if err != nil {
		return NewInternalErrorf("can't enforce preconditions %v on un-introspectable object %v, got error: %v", *preconditions, out, err)
	}
	if preconditions.UID != nil && *preconditions.UID != objMeta.UID {
		return NewResourceVersionConflictsError(key, int64(rv)) 
	}
	return nil
}

// APIObjectVersioner implements Versioner
var _ Versioner = APIObjectVersioner{}
var _ Interface = &genericWrapper{}
var _ watch.Interface = &genericWatcher{}