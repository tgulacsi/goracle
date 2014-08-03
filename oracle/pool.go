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

/*
#cgo LDFLAGS: -lclntsh

#include <oci.h>
*/
import "C"

import (
	"unsafe"
)

// ConnectionPool is a connection pool interface
type ConnectionPool interface {
	// Get returns a new connection
	Get() (*Connection, error)
	// Put puts the connection back to the pool
	Put(*Connection)
	// Close closes the pool
	Close() error
}

type goConnectionPool struct {
	pool                    chan *Connection
	username, password, sid string
}

// NewGoConnectionPool returns a simple sync.Pool-backed ConnectionPool.
func NewGoConnectionPool(username, password, sid string, connMax int) (ConnectionPool, error) {
	if connMax <= 0 {
		connMax = 999
	}
	return &goConnectionPool{
		pool:     make(chan *Connection, connMax),
		username: username, password: password, sid: sid,
	}, nil
}

func (cp *goConnectionPool) Get() (*Connection, error) {
	select {
	case c := <-cp.pool:
		return c, nil
	default:
		return NewConnection(cp.username, cp.password, cp.sid, false)
	}
}

func (cp *goConnectionPool) Put(conn *Connection) {
	select {
	case cp.pool <- conn:
		// in chan
	default:
		conn.Close()
	}
}

func (cp *goConnectionPool) Close() error {
	if cp == nil || cp.pool == nil {
		return nil
	}
	close(cp.pool)
	for c := range cp.pool {
		c.Close()
	}
	cp.pool = nil
	return nil
}

// ConnectionPool holds C.OCICPool for connection pooling
type oraConnectionPool struct {
	handle      *C.OCICPool
	authHandle  *C.OCIAuthInfo
	environment *Environment
	name        string
}

// NewORAConnectionPool returns a new connection pool wrapping an OCI Connection Pool.
func NewORAConnectionPool(username, password, dblink string, connMin, connMax, connIncr int) (ConnectionPool, error) {
	env, err := NewEnvironment()
	if err != nil {
		return nil, err
	}
	var pool oraConnectionPool
	if err = ociHandleAlloc(unsafe.Pointer(env.handle),
		C.OCI_HTYPE_CPOOL, (*unsafe.Pointer)(unsafe.Pointer(&pool.handle)),
		"pool.handle"); err != nil || pool.handle == nil {
		return nil, err
	}

	if err = ociHandleAlloc(unsafe.Pointer(env.handle),
		C.OCI_HTYPE_AUTHINFO, (*unsafe.Pointer)(unsafe.Pointer(&pool.authHandle)),
		"pool.authHandle"); err != nil || pool.authHandle == nil {
		C.OCIHandleFree(unsafe.Pointer(pool.handle), C.OCI_HTYPE_CPOOL)
		return nil, err
	}
	defer func() {
		if err != nil {
			C.OCIHandleFree(unsafe.Pointer(pool.authHandle), C.OCI_HTYPE_AUTHINFO)
			C.OCIHandleFree(unsafe.Pointer(pool.handle), C.OCI_HTYPE_CPOOL)
		}
	}()
	if username != "" {
		if err = env.AttrSet(unsafe.Pointer(pool.authHandle), C.OCI_HTYPE_AUTHINFO,
			C.OCI_ATTR_USERNAME,
			unsafe.Pointer(&[]byte(username)[0]), len(username)); err != nil {
			return nil, err
		}
	}
	if password != "" {
		if err = env.AttrSet(unsafe.Pointer(pool.authHandle), C.OCI_HTYPE_AUTHINFO,
			C.OCI_ATTR_PASSWORD,
			unsafe.Pointer(&[]byte(password)[0]), len(password)); err != nil {
			return nil, err
		}
	}

	var (
		nameP   unsafe.Pointer
		nameLen C.sb4
	)
	if err = env.CheckStatus(
		C.OCIConnectionPoolCreate(env.handle, env.errorHandle, pool.handle,
			(**C.OraText)(unsafe.Pointer(&nameP)), &nameLen,
			(*C.OraText)(unsafe.Pointer(&([]byte(dblink)[0]))), C.sb4(len(dblink)),
			C.ub4(connMin), C.ub4(connMax), C.ub4(connIncr),
			(*C.OraText)(unsafe.Pointer(&([]byte(username)[0]))), C.sb4(len(username)),
			(*C.OraText)(unsafe.Pointer(&([]byte(password)[0]))), C.sb4(len(password)),
			C.OCI_DEFAULT),
		"CreateConnectionPool"); err != nil {
		return nil, err
	}
	pool.name = C.GoStringN((*C.char)(nameP), C.int(nameLen))
	pool.environment = env

	return &pool, nil
}

// Close the connection pool.
func (cp *oraConnectionPool) Close() error {
	cp.authHandle = nil
	if cp.handle == nil {
		return nil
	}
	err := cp.environment.CheckStatus(
		C.OCIConnectionPoolDestroy(cp.handle, cp.environment.errorHandle, C.OCI_DEFAULT),
		"ConnectionPoolDestroy")
	C.OCIHandleFree(unsafe.Pointer(cp.handle), C.OCI_HTYPE_CPOOL)
	cp.handle = nil
	return err
}

// Acquire a new connection.
// On Close of this returned connection, it will only released back to the pool.
func (cp *oraConnectionPool) Get() (*Connection, error) {
	conn := &Connection{connectionPool: cp, environment: cp.environment}
	if err := cp.environment.CheckStatus(
		C.OCISessionGet(cp.environment.handle, cp.environment.errorHandle,
			&conn.handle, cp.authHandle,
			(*C.OraText)(unsafe.Pointer(&([]byte(cp.name))[0])), C.ub4(len(cp.name)),
			nil, 0, nil, nil, nil,
			C.OCI_SESSGET_CPOOL),
		"SessionGet"); err != nil {
		return nil, err
	}
	return conn, nil
}

// Release a connection back to the pool.
func (cp *oraConnectionPool) Put(conn *Connection) {
	if conn == nil || conn.handle == nil || conn.connectionPool == nil || !conn.IsConnected() {
		return
	}
	conn.srvMtx.Lock()
	defer conn.srvMtx.Unlock()
	err := cp.environment.CheckStatus(
		C.OCISessionRelease(conn.handle, cp.environment.errorHandle, nil, 0, C.OCI_DEFAULT),
		"SessionRelease")
	if err != nil {
		conn.connectionPool = nil
		conn.close()
		conn.handle = nil
	}
	return
}
