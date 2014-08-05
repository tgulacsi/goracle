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
	"fmt"
	"sync"
	"testing"
	"time"
)

var poolSize = 10

func TestBoundedConnPool(t *testing.T) {
	user, passw, sid := SplitDSN(*dsn)
	/*
		pool, err := NewBoundedConnPool(user, passw, sid, 2, poolSize, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		testConnPool(t, pool)
	*/

	pool, err := NewBoundedConnPool(user, passw, sid, 2, poolSize, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	testConnPool(t, pool)
}

func TestGoConnPool(t *testing.T) {
	user, passw, sid := SplitDSN(*dsn)
	pool, err := NewGoConnectionPool(user, passw, sid, poolSize)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	testConnPool(t, pool)
}

func TestORAConnPool(t *testing.T) {
	user, passw, sid := SplitDSN(*dsn)
	pool, err := NewORAConnectionPool(user, passw, sid, 1, poolSize, 1)
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

func TestSmallGoPool(t *testing.T) {
	user, passw, sid := SplitDSN(*dsn)
	var err error
	pool, err = NewGoConnectionPool(user, passw, sid, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	c1 := getConnection(t)
	st := pool.Stats()
	t.Logf("1. %s", st)
	if st.InUse != 1 {
		t.Errorf("awaited inUse=1, got %d", st.InUse)
	}

	c2 := getConnection(t)
	st = pool.Stats()
	t.Logf("2. %s", st)
	if st.InUse != 2 {
		t.Errorf("awaited inUse=2, got %d", st.InUse)
	}

	if err = c1.Close(); err != nil {
		t.Errorf("close c1: %v", err)
	}
	st = pool.Stats()
	t.Logf("close c1: %s", st)
	if st.InUse != 1 || st.InIdle != 1 {
		t.Errorf("awaited (inUse, inIdle) = (1, 1), got (%d, %d)", st.InUse, st.InIdle)
	}

	if err = c2.Close(); err != nil {
		t.Errorf("close c2: %v", err)
	}
	st = pool.Stats()
	t.Logf("close c2: %s", st)
	if st.InUse != 0 || st.InIdle != 1 || st.PoolTooSmall != 1 {
		t.Errorf("awaited (inUse, inIdle, PoolTooSmall) = (0, 1, 1), got (%d, %d, %d)", st.InUse, st.InIdle, st.PoolTooSmall)
	}
}

func TestSmallORAConnPool(t *testing.T) {
	user, passw, sid := SplitDSN(*dsn)
	var err error
	pool, err = NewORAConnectionPool(user, passw, sid, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	conns := []*Connection{getConnection(t), nil}
	st := pool.Stats()
	t.Logf("1. %s", st)
	if st.InUse != 1 {
		t.Errorf("awaited inUse=1, got %d", st.InUse)
	}

	ch := make(chan struct{})
	go func() {
		var err error
		if conns[1], err = pool.Get(); err != nil {
			t.Errorf("error geting connection from pool: %v", err)
			t.FailNow()
		}
		ch <- struct{}{}
	}()
	select {
	case <-ch:
		handles := make([]string, len(conns))
		for i, c := range conns {
			handles[i] = fmt.Sprintf("%p", c.handle)
		}
		t.Errorf("pool got beyond its capacity! awaited 1 connections, got %#v", handles)
		time.Sleep(10 * time.Second)
	case <-time.After(time.Second):
	}
}

func TestSmallBoundedPool(t *testing.T) {
	user, passw, sid := SplitDSN(*dsn)
	var err error
	pool, err = NewBoundedConnPool(user, passw, sid, 1, 1, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	conns := []*Connection{getConnection(t), nil}
	st := pool.Stats()
	t.Logf("1. %s", st)

	conns[1], err = pool.Get()
	t.Logf("error for overacquiring the pool: %v", err)
	st = pool.Stats()
	t.Logf("2. %s", st)
	if err == nil {
		t.Errorf("pool got beyond its capacity! awaited 1 connections, got %#v", conns)
	} else if err != ErrPoolTimeout {
		t.Errorf("pool error: %v", err)
	}
}
