/*
Copyright 2015 The Kubernetes Authors All rights reserved.

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

package etcd

import (
	"testing"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/fields"
	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/registry/generic"
	"k8s.io/kubernetes/pkg/registry/registrytest"
	"k8s.io/kubernetes/pkg/storage/etcd/etcdtest"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/api/testapi"
	//etcdtesting "k8s.io/kubernetes/pkg/storage/etcd/testing"
	"k8s.io/kubernetes/pkg/util/diff"
	storagetesting "k8s.io/kubernetes/pkg/storage/testing/factory"
	"path"
)

func newStorage(t *testing.T, factory storagetesting.TestServerFactory) (*REST, *StatusREST, storagetesting.TestServer) {
	server := factory.NewTestClientServer(t)
	prefix := path.Join("/", etcdtest.PathPrefix())
	genericStore := storage.NewGenericWrapper(server.NewRawStorage(), testapi.Default.Codec(), prefix)
	restOptions := generic.RESTOptions{Storage: genericStore, Decorator: generic.UndecoratedStorage, DeleteCollectionWorkers: 1}
	resourceQuotaStorage, statusStorage := NewREST(restOptions)
	return resourceQuotaStorage, statusStorage, server
}

func validNewResourceQuota() *api.ResourceQuota {
	return &api.ResourceQuota{
		ObjectMeta: api.ObjectMeta{
			Name:      "foo",
			Namespace: api.NamespaceDefault,
		},
		Spec: api.ResourceQuotaSpec{
			Hard: api.ResourceList{
				api.ResourceCPU:                    resource.MustParse("100"),
				api.ResourceMemory:                 resource.MustParse("4Gi"),
				api.ResourcePods:                   resource.MustParse("10"),
				api.ResourceServices:               resource.MustParse("10"),
				api.ResourceReplicationControllers: resource.MustParse("10"),
				api.ResourceQuotas:                 resource.MustParse("1"),
			},
		},
	}
}

func TestStorage(t *testing.T) {
	factories := storagetesting.GetAllTestStorageFactories(t)[1:]

	for _, factory := range factories {
		t.Logf("testing with storage implementation: %s", factory.GetName())
		testCreate(t, factory)
		testCreateSetsFields(t, factory)
		testDelete(t, factory)
		testGet(t, factory)

		testList(t, factory)
		testWatch(t, factory)
		testUpdateStatus(t, factory)
	}
}


func testCreate(t *testing.T, factory storagetesting.TestServerFactory) {
	storage, _, server := newStorage(t, factory)
	defer server.Terminate(t)
	test := registrytest.New(t, storage.Etcd)
	resourcequota := validNewResourceQuota()
	resourcequota.ObjectMeta = api.ObjectMeta{}
	test.TestCreate(
		// valid
		resourcequota,
		// invalid
		&api.ResourceQuota{
			ObjectMeta: api.ObjectMeta{Name: "_-a123-a_"},
		},
	)
}

func testCreateSetsFields(t *testing.T, factory storagetesting.TestServerFactory) {
	storage, _, server := newStorage(t, factory)
	defer server.Terminate(t)
	ctx := api.NewDefaultContext()
	resourcequota := validNewResourceQuota()
	_, err := storage.Create(api.NewDefaultContext(), resourcequota)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	object, err := storage.Get(ctx, "foo")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	actual := object.(*api.ResourceQuota)
	if actual.Name != resourcequota.Name {
		t.Errorf("unexpected resourcequota: %#v", actual)
	}
	if len(actual.UID) == 0 {
		t.Errorf("expected resourcequota UID to be set: %#v", actual)
	}
}

func testDelete(t *testing.T, factory storagetesting.TestServerFactory) {
	storage, _, server := newStorage(t, factory)
	defer server.Terminate(t)
	test := registrytest.New(t, storage.Etcd).ReturnDeletedObject()
	test.TestDelete(validNewResourceQuota())
}

func testGet(t *testing.T, factory storagetesting.TestServerFactory) {
	storage, _, server := newStorage(t, factory)
	defer server.Terminate(t)
	test := registrytest.New(t, storage.Etcd)
	test.TestGet(validNewResourceQuota())
}

func testList(t *testing.T, factory storagetesting.TestServerFactory) {
	storage, _, server := newStorage(t, factory)
	defer server.Terminate(t)
	test := registrytest.New(t, storage.Etcd)
	test.TestList(validNewResourceQuota())
}

func testWatch(t *testing.T, factory storagetesting.TestServerFactory) {
	storage, _, server := newStorage(t, factory)
	defer server.Terminate(t)
	test := registrytest.New(t, storage.Etcd)
	test.TestWatch(
		validNewResourceQuota(),
		// matching labels
		[]labels.Set{},
		// not matching labels
		[]labels.Set{
			{"foo": "bar"},
		},
		// matching fields
		[]fields.Set{
			{"metadata.name": "foo"},
		},
		// not matchin fields
		[]fields.Set{
			{"metadata.name": "bar"},
		},
	)
}

func testUpdateStatus(t *testing.T, factory storagetesting.TestServerFactory) {
	storage, status, server := newStorage(t, factory)
	defer server.Terminate(t)
	ctx := api.NewDefaultContext()

	key, _ := storage.KeyFunc(ctx, "foo")
	key = etcdtest.AddPrefix(key)
	resourcequotaStart := validNewResourceQuota()
	err := storage.Storage.Create(ctx, key, resourcequotaStart, nil, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	resourcequotaIn := &api.ResourceQuota{
		ObjectMeta: api.ObjectMeta{
			Name:      "foo",
			Namespace: api.NamespaceDefault,
		},
		Status: api.ResourceQuotaStatus{
			Used: api.ResourceList{
				api.ResourceCPU:                    resource.MustParse("1"),
				api.ResourceMemory:                 resource.MustParse("1Gi"),
				api.ResourcePods:                   resource.MustParse("1"),
				api.ResourceServices:               resource.MustParse("1"),
				api.ResourceReplicationControllers: resource.MustParse("1"),
				api.ResourceQuotas:                 resource.MustParse("1"),
			},
			Hard: api.ResourceList{
				api.ResourceCPU:                    resource.MustParse("100"),
				api.ResourceMemory:                 resource.MustParse("4Gi"),
				api.ResourcePods:                   resource.MustParse("10"),
				api.ResourceServices:               resource.MustParse("10"),
				api.ResourceReplicationControllers: resource.MustParse("10"),
				api.ResourceQuotas:                 resource.MustParse("1"),
			},
		},
	}

	_, _, err = status.Update(ctx, resourcequotaIn)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	obj, err := storage.Get(ctx, "foo")
	rqOut := obj.(*api.ResourceQuota)
	// only compare the meaningful update b/c we can't compare due to metadata
	if !api.Semantic.DeepEqual(resourcequotaIn.Status, rqOut.Status) {
		t.Errorf("unexpected object: %s", diff.ObjectDiff(resourcequotaIn, rqOut))
	}
}
