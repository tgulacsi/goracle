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
	"log"
	"testing"
)

func TestConnPool(t *testing.T) {
	c1 := getPooledConn(t)
	t.Logf("pooled conn 1: %#v", c1)
	c2 := getPooledConn(t)
	t.Logf("pooled conn 2: %#v", c2)
	if err := c1.Close(); err != nil {
		t.Errorf("close c1: %v", err)
	}
	c3 := getPooledConn(t)
	t.Logf("pooled conn 3: %#v", c3)
	if err := c2.Close(); err != nil {
		t.Errorf("close c2: %v", err)
	}
	if err := c3.Close(); err != nil {
		t.Errorf("close c3: %v", err)
	}
}

var pool *ConnectionPool

func getPooledConn(t *testing.T) *Connection {
	var err error
	if pool == nil {
		user, passw, sid := SplitDSN(*dsn)
		pool, err = NewConnectionPool(user, passw, sid, 1, 100, 1)
		if err != nil {
			log.Panicf("error creating connection pool to %s: %v", *dsn, err)
		}
	}
	conn, err = pool.Acquire()
	if err != nil {
		log.Panicf("error acquiring connection from %v: %v", pool, err)
	}
	if err = conn.Connect(0, false); err != nil {
		log.Panicf("error connecting: %s", err)
	}
	return conn
}
