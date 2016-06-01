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

package etcd3

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/meta"
	"k8s.io/kubernetes/pkg/conversion"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/etcd"
	"k8s.io/kubernetes/pkg/watch"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"k8s.io/kubernetes/pkg/storage/generic"
	"time"
)

type rawStore struct {
	client     *clientv3.Client
	codec      runtime.Codec
	versioner  storage.Versioner
	pathPrefix string
	watcher    *watcher
}

func (s *rawStore) Create(ctx context.Context, key string, data []byte, raw *generic.RawObject, ttl uint64) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}

	opts, err := s.ttlOpts(ctx, int64(ttl))
	if err != nil {
		return err
	}

	txnResp, err := s.client.KV.Txn(ctx).If(
		notFound(key),
	).Then(
		clientv3.OpPut(key, string(data), opts...),
	).Commit()
	if err != nil {
		return err
	}
	if !txnResp.Succeeded {
		return storage.NewKeyExistsError(key, 0)
	}

	if raw != nil {
		putResp := txnResp.Responses[0].GetResponsePut()
		putResp.MarshalTo(raw.Data)
		raw.Version = uint64(putResp.Header.Revision)

		// TODO: Handle TTL
		// raw.TTL = ...
		return err
	}
	return nil
}

// New returns an etcd3 implementation of storage.Interface.
func NewGenericRaw(c *clientv3.Client, codec runtime.Codec, prefix string) generic.InterfaceRaw {
	return newRawStore(c, codec, prefix)
}

// FIXME
func newRawStore(c *clientv3.Client, codec runtime.Codec, prefix string) *rawStore {
	versioner := etcd.APIObjectVersioner{}
	return &rawStore{
		client:     c,
		versioner:  versioner,
		codec:      codec,
		pathPrefix: prefix,
		watcher:    newWatcher(c, codec, versioner),
	}
}

// Backends implements storage.Interface.Backends.
func (s *rawStore) Backends(ctx context.Context) []string {
	resp, err := s.client.MemberList(ctx)
	if err != nil {
		glog.Errorf("Error obtaining etcd members list: %q", err)
		return nil
	}
	var mlist []string
	for _, member := range resp.Members {
		mlist = append(mlist, member.ClientURLs...)
	}
	return mlist
}

// Codec implements storage.Interface.Codec.
func (s *rawStore) Codec() runtime.Codec {
	return s.codec
}

// Versioner implements storage.Interface.Versioner.
func (s *rawStore) Versioner() storage.Versioner {
	return s.versioner
}

// Get implements storage.Interface.Get.
func (s *rawStore) Get(ctx context.Context, key string, raw *generic.RawObject) error {
	key = keyWithPrefix(s.pathPrefix, key)
	getResp, err := s.client.KV.Get(ctx, key)
	if err != nil {
		return err
	}

	if len(getResp.Kvs) == 0 {
		return storage.NewKeyNotFoundError(key, 0)
	}
	kv := getResp.Kvs[0]
	return nil
}

// Delete implements storage.Interface.Delete.
func (s *rawStore) Delete(ctx context.Context, key string, raw *generic.RawObject, preconditions generic.RawFilterFunc) error {
	v, err := conversion.EnforcePtr(raw)
	if err != nil {
		panic("unable to convert output object to pointer")
		key = keyWithPrefix(s.pathPrefix, key)
		if preconditions == nil {
			return s.unconditionalDeleteRaw(ctx, key, raw)
		}
		return s.conditionalDeleteRaw(ctx, key, raw, v, &preconditions)
	}
	return nil
}

func (s *rawStore) unconditionalDeleteRaw(ctx context.Context, key string, raw *generic.RawObject) error {
	// We need to do get and delete in single transaction in order to
	// know the value and revision before deleting it.
	txnResp, err := s.client.KV.Txn(ctx).If().Then(
		clientv3.OpGet(key),
		clientv3.OpDelete(key),
	).Commit()
	if err != nil {
		return err
	}
	getResp := txnResp.Responses[0].GetResponseRange()
	if len(getResp.Kvs) == 0 {
		return storage.NewKeyNotFoundError(key, 0)
	}

	kv := getResp.Kvs[0]
	raw.Version = uint64(kv.ModRevision)
	_, err = kv.MarshalTo(raw.Data)
	return err
}

// Preconditions is unused because this function relies on etcd3 transactions to achieve the same behavior
func (s *rawStore) conditionalDeleteRaw(ctx context.Context, key string, out *generic.RawObject, v reflect.Value, preconditions generic.RawFilterFunc) error {

	getResp, err := s.client.KV.Get(ctx, key)
	if err != nil {
		return err
	}
	for {
		origState, err := s.getState(getResp, key, v, false)
		if err != nil {
			return err
		}

		txnResp, err := s.client.KV.Txn(ctx).If(
			clientv3.Compare(clientv3.ModRevision(key), "=", origState.rev),
		).Then(
			clientv3.OpDelete(key),
		).Else(
			clientv3.OpGet(key),
		).Commit()
		if err != nil {
			return err
		}
		if !txnResp.Succeeded {
			getResp = (*clientv3.GetResponse)(txnResp.Responses[0].GetResponseRange())
			glog.V(4).Infof("deletion of %s failed because of a conflict, going to retry", key)
			continue
		}
		resp := txnResp.Responses[0].GetResponseDeleteRange()
		out.Version = uint64(resp.Header.Revision)
		_, err = resp.MarshalTo(out.Data)
		return nil
	}
}

// GuaranteedUpdate implements storage.Interface.GuaranteedUpdate.
/*func (s *rawStore) GuaranteedUpdate(ctx context.Context, key string, out runtime.Object, ignoreNotFound bool, precondtions *storage.Preconditions, tryUpdate storage.UpdateFunc) error {
	v, err := conversion.EnforcePtr(out)
	if err != nil {
		panic("unable to convert output object to pointer")
	}
	key = keyWithPrefix(s.pathPrefix, key)
	getResp, err := s.client.KV.Get(ctx, key)
	if err != nil {
		return err
	}
	for {
		origState, err := s.getState(getResp, key, v, ignoreNotFound)
		if err != nil {
			return err
		}

		if err := checkPreconditions(key, precondtions, origState.obj); err != nil {
			return err
		}

		ret, ttl, err := s.updateState(origState, tryUpdate)
		if err != nil {
			return err
		}

		data, err := runtime.Encode(s.codec, ret)
		if err != nil {
			return err
		}
		if bytes.Equal(data, origState.data) {
			return decode(s.codec, s.versioner, origState.data, out, origState.rev)
		}

		opts, err := s.ttlOpts(ctx, int64(ttl))
		if err != nil {
			return err
		}

		txnResp, err := s.client.KV.Txn(ctx).If(
			clientv3.Compare(clientv3.ModRevision(key), "=", origState.rev),
		).Then(
			clientv3.OpPut(key, string(data), opts...),
		).Else(
			clientv3.OpGet(key),
		).Commit()
		if err != nil {
			return err
		}
		if !txnResp.Succeeded {
			getResp = (*clientv3.GetResponse)(txnResp.Responses[0].GetResponseRange())
			glog.V(4).Infof("GuaranteedUpdate of %s failed because of a conflict, going to retry", key)
			continue
		}
		putResp := txnResp.Responses[0].GetResponsePut()
		return decode(s.codec, s.versioner, data, out, putResp.Header.Revision)
	}
}
*/

// GetToList implements storage.Interface.GetToList.
func (s *rawStore) GetToList(ctx context.Context, key string, rawList *[]generic.RawObject) (uint64, error) {
	getResp, err := s.client.KV.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	if len(getResp.Kvs) == 0 {
		return 0, nil
	}

	var rawObj generic.RawObject
	resp := getResp.Kvs[0]
	resp.MarshalTo(rawObj.Data)
	rawObj.Version = uint64(resp.ModRevision)
	// TODO: Handle TTL?
	if len(rawObj.Data) > 0 {
		*rawList = append(*rawList, rawObj)
	}

	// TODO: what is the uint64 retval? NOT SURE ABOU THIS
	return uint64(resp.Version), nil
}

// List implements storage.Interface.List.
func (s *rawStore) List(ctx context.Context, key string, resourceVersion string, rawList *[]generic.RawObject) (uint64, error) {
	// We need to make sure the key ended with "/" so that we only get children "directories".
	// e.g. if we have key "/a", "/a/b", "/ab", getting keys with prefix "/a" will return all three,
	// while with prefix "/a/" will return only "/a/b" which is the correct answer.
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	getResp, err := s.client.KV.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}

	elems := make([]*generic.RawObject, len(getResp.Kvs))
	for i, kv := range getResp.Kvs {
		elems[i] = generic.RawObject{}
		kv.MarshalTo(elems[i].Data)
		elems[i].Version = kv.ModRevision
		// elems[i].TTL = kv. // TODO: handle TTL
		// TODO: handle UID?
	}
	rawList = elems

	return uint64(getResp.Header.Revision), nil
}

// Watch implements storage.Interface.Watch.
func (s *rawStore) Watch(ctx context.Context, key string, resourceVersion string) (watch.Interface, error) {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	watchRV, err := storage.ParseWatchResourceVersion(resourceVersion)
	if err != nil {
		return nil, err
	}
	ret := newEtcd3WatcherRaw(false, s.quorum, nil)
	go ret.etcdWatch(ctx, s.etcdKeysAPI, key, watchRV)

	return s.watch(ctx, key, resourceVersion, filter, false)
}

// WatchList implements storage.Interface.WatchList.
func (s *rawStore) WatchList(ctx context.Context, key string, resourceVersion string, filter storage.FilterFunc) (watch.Interface, error) {
	return s.watch(ctx, key, resourceVersion, filter, true)
}

func (s *rawStore) watch(ctx context.Context, key string, rv string, filter storage.FilterFunc, recursive bool) (watch.Interface, error) {
	rev, err := storage.ParseWatchResourceVersion(rv)
	if err != nil {
		return nil, err
	}
	key = keyWithPrefix(s.pathPrefix, key)
	return s.watcher.Watch(ctx, key, int64(rev), recursive, filter)
}

func (s *rawStore) getState(getResp *clientv3.GetResponse, key string, v reflect.Value, ignoreNotFound bool) (*objState, error) {
	state := &objState{
		obj:  reflect.New(v.Type()).Interface().(runtime.Object),
		meta: &storage.ResponseMeta{},
	}
	if len(getResp.Kvs) == 0 {
		if !ignoreNotFound {
			return nil, storage.NewKeyNotFoundError(key, 0)
		}
		if err := runtime.SetZeroValue(state.obj); err != nil {
			return nil, err
		}
	} else {
		state.rev = getResp.Kvs[0].ModRevision
		state.meta.ResourceVersion = uint64(state.rev)
		state.data = getResp.Kvs[0].Value
		if err := decode(s.codec, s.versioner, state.data, state.obj, state.rev); err != nil {
			return nil, err
		}
	}
	return state, nil
}

func (s *rawStore) updateState(st *objState, userUpdate storage.UpdateFunc) (runtime.Object, uint64, error) {
	ret, ttlPtr, err := userUpdate(st.obj, *st.meta)
	if err != nil {
		return nil, 0, err
	}

	version, err := s.versioner.ObjectResourceVersion(ret)
	if err != nil {
		return nil, 0, err
	}
	var ttl uint64
	var expiration *time.Time
	if ttlPtr != nil {
		ttl = *ttlPtr
		expireTime := time.Now().Add(time.Duration(ttl) * time.Second)
		expiration = &expireTime
	}
	if version != 0 {
		// We cannot store object with resourceVersion in etcd. We need to reset it.
		if err := s.versioner.UpdateObject(ret, expiration, 0); err != nil {
			return nil, 0, fmt.Errorf("UpdateObject failed: %v", err)
		}
	}
	return ret, ttl, nil
}

// ttlOpts returns client options based on given ttl.
// ttl: if ttl is non-zero, it will attach the key to a lease with ttl of roughly the same length
func (s *rawStore) ttlOpts(ctx context.Context, ttl int64) ([]clientv3.OpOption, error) {
	if ttl == 0 {
		return nil, nil
	}
	// TODO: one lease per ttl key is expensive. Based on current use case, we can have a long window to
	// put keys within into same lease. We shall benchmark this and optimize the performance.
	lcr, err := s.client.Lease.Grant(ctx, ttl)
	if err != nil {
		return nil, err
	}
	return []clientv3.OpOption{clientv3.WithLease(clientv3.LeaseID(lcr.ID))}, nil
}

func notFound(key string) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(key), "=", 0)
}

func Set(ctx context.Context, key string, raw *generic.RawObject) (bool, error) {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	if raw == nil {
		return false, nil // maybe this should be an error.. a well behaved client should ask us to Set nothing
	}
	opts := etcd.SetOptions{
		PrevIndex: raw.Version,
		TTL:       time.Duration(raw.TTL) * time.Second,
	}
	if raw.Version == 0 {
		opts.PrevExist = etcd.PrevNoExist
	}
	response, err := s.etcdKeysAPI.Set(ctx, key, string(raw.Data), &opts)
	copyResponse(response, raw, false)
	if err != nil {
		if etcdutil.IsEtcdTestFailed(err) || (raw.Version == 0 && etcdutil.IsEtcdNodeExist(err)) {
			return false, nil
		}
		return false, toStorageErr(err, key, 0)
	}
	return true, nil
}
