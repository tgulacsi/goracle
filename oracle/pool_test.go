/*
Copyright 2014 Tamás Gulácsi

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

package oracle

import (
	"sync"
	"testing"
)

var poolSize = 10

func TestGoConnPool(t *testing.T) {
	user, passw, sid := SplitDSN(*dsn)
	pool, err := NewGoConnectionPool(user, passw, sid, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	testConnPool(t, pool)
}

func TestORAConnPool(t *testing.T) {
	user, passw, sid := SplitDSN(*dsn)
	pool, err := NewORAConnectionPool(user, passw, sid, 1, 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	testConnPool(t, pool)
}

func testConnPool(t *testing.T, p ConnectionPool) {
	pool = p // global pool, used by getConnection!
	t.Logf("pool stats: %s", pool.Stats())
	c1 := getConnection(t)
	t.Logf("pool stats: %s", pool.Stats())
	t.Logf("pooled conn 1: %#v", c1)
	if err := c1.NewCursor().Execute("SELECT 1 FROM DUAL", nil, nil); err != nil {
		t.Errorf("bad connection: %v", err)
	}
	c2 := getConnection(t)
	t.Logf("pool stats: %s", pool.Stats())
	t.Logf("pooled conn 2: %#v", c2)
	if err := c1.Close(); err != nil {
		t.Errorf("close c1: %v", err)
	}
	c3 := getConnection(t)
	t.Logf("pool stats: %s", pool.Stats())
	t.Logf("pooled conn 3: %#v", c3)
	if err := c2.Close(); err != nil {
		t.Errorf("close c2: %v", err)
	}

	if err := c3.Close(); err != nil {
		t.Errorf("close c3: %v", err)
	}
	t.Logf("pool stats: %s", pool.Stats())

	TestOutBinds(t)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		TestSimpleTypes(t)
	}()
	wg.Wait()
	t.Logf("pool stats: %s", pool.Stats())

}
