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

package consul

import "github.com/hashicorp/consul/api"

type store struct {
}

// Get implements storage.Interface.Get.
func (s *store) Get() error {
	client, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		panic(err)
	}
	kv := client.KV()
	kv.Get("foo", nil)

	return err
}
