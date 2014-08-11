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
	//"expvar"  // for pool statistics
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/tgulacsi/goracle/third_party/vitess/pools"
)

// IdleTimeout is the idle timeout.
const IdleTimeout = 1 * time.Minute

var (
	// ErrPoolTimeout is returned by Get if timeout has been set and the operation times out.
	ErrPoolTimeout = errors.New("pool timed out")

	// ErrPoolIsClosed is returned by Get if pool is closed
	ErrPoolIsClosed = errors.New("pool is closed")
)

// ConnectionPool is a connection pool interface
type ConnectionPool interface {
	// Get returns a new connection
	Get() (*Connection, error)
	// Put puts the connection back to the pool
	Put(*Connection)
	// Close closes the pool
	Close() error
	// Stats returns pool statistics
	Stats() Statistics
}

type vitessPool struct {
	*pools.ResourcePool
	getCh     chan maybeConn
	putCh     chan *Connection
	closeCh   chan struct{}
	timeout   time.Duration
	stats     Statistics
	wg        sync.WaitGroup
	closedMtx sync.Mutex
	closed    bool
}
type vitessConn struct {
	conn *Connection
}

func (vc vitessConn) Close() {
	vc.conn.srvMtx.Lock()
	vc.conn.connectionPool = nil
	vc.conn.srvMtx.Unlock()
	_ = vc.conn.Close()
}

type maybeConn struct {
	conn *Connection
	err  error
}

// NewBoundedConnPool returns a new bounded connection pool. It is based on vitess/pools.
//
// If connMax < 0 then it will web 999.
func NewBoundedConnPool(username, password, sid string, connMax int, timeout time.Duration) (ConnectionPool, error) {
	pool := &vitessPool{timeout: timeout, stats: Statistics{PoolCap: uint32(connMax)}}
	var factory = pools.Factory(func() (pools.Resource, error) {
		c, err := NewConnection(username, password, sid, false)
		if err != nil {
			return nil, err
		}
		c.connectionPool = pool
		return vitessConn{c}, err
	})
	if connMax < 1 {
		connMax = 999
	}
	rp := pools.NewResourcePool(factory, connMax, connMax, IdleTimeout) // !! connMax !!
	pool.ResourcePool = rp
	if timeout > 0 {
		pool.getCh = make(chan maybeConn)
		pool.putCh = make(chan *Connection)
		pool.closeCh = make(chan struct{})

		// GET
		pool.wg.Add(1)
		go func() {
			defer pool.wg.Done()
			defer close(pool.getCh)
			for {
				res, err := rp.Get()
				var c *Connection
				if err == nil {
					c = res.(vitessConn).conn
					c.connectionPool = pool
				}
				// see http://dave.cheney.net/2014/03/19/channel-axioms
				select {
				case <-pool.closeCh:
					if c != nil {
						c.connectionPool = nil
						c.Close()
					}
					return
				case pool.getCh <- maybeConn{c, err}:
				}
			}
		}()

		// PUT
		pool.wg.Add(1)
		go func() {
			defer pool.wg.Done()
			for {
				select {
				case <-pool.closeCh:
					return
				case c := <-pool.putCh:
					var vc pools.Resource
					if c != nil {
					c.srvMtx.Lock()
					c.connectionPool = nil
					c.srvMtx.Unlock()
					if c.IsConnected() {
						vc = vitessConn{c}
					}}
					func(vc pools.Resource) {
						defer func() {
							recover()
						}()
						rp.Put(nil)
					}(vc)
				}
			}
		}()
	}
	// ResourcePool discards old connections only on get
	go func() {
		for _ = range time.Tick(IdleTimeout) {
			if rp.IsClosed() {
				return
			}
			for i := int64(0); i < rp.Available(); i++ {
				res, err := rp.TryGet()
				if err != nil || res == nil {
					break
				}
				rp.Put(res)
			}
		}
	}()
	return pool, nil
}

func (p *vitessPool) Close() error {
	go p.ResourcePool.Close()
	go func() {
		defer func() {
			recover()
		}()
		for i := p.ResourcePool.Capacity() - p.ResourcePool.Available(); i > 0; i-- {
			p.ResourcePool.Put(nil)
		}
	}()
	p.closedMtx.Lock()
	if !p.closed {
		close(p.putCh)
		go func() {
			defer close(p.closeCh)
			for i := 0; i < 2; i++ {
				p.closeCh <- struct{}{}
			}
		}()
		p.closed = true
	}
	p.closedMtx.Unlock()
	p.wg.Wait()
	return nil
}

func (p *vitessPool) Get() (*Connection, error) {
	add(&p.stats.InUse, 1, &p.stats.MaxUse)
	if p.getCh != nil {
		select {
		case mc := <-p.getCh:
			atomic.AddUint32(&p.stats.InIdle, ^uint32(0)) // decrement
			return mc.conn, mc.err
		case <-time.After(p.timeout):
			return nil, ErrPoolTimeout
		}
	}

	res, err := p.ResourcePool.Get()
	if err != nil {
		return nil, err
	}
	atomic.AddUint32(&p.stats.InIdle, ^uint32(0)) // decrement
	return res.(vitessConn).conn, nil
}

func (p *vitessPool) Put(conn *Connection) {
	atomic.AddUint32(&p.stats.InUse, ^uint32(0)) // decremenet
	if p.putCh != nil {
		select {
		case p.putCh <- conn:
			add(&p.stats.InIdle, 1, &p.stats.MaxIdle)
			return
		case <-time.After(p.timeout):
			atomic.AddUint32(&p.stats.PoolTooSmall, 1)
			conn.connectionPool = nil
			conn.Close()
			return
		}
	}

	p.ResourcePool.Put(vitessConn{conn})
}

func (p *vitessPool) Stats() Statistics {
	stats := p.stats
	stats.InIdle = uint32(p.ResourcePool.Capacity())
	return stats
	//return Statistics{InIdle: uint32(p.ResourcePool.Capacity()), PoolCap: uint32(p.ResourcePool.MaxCap())}
}

// Statistics is a collection of pool usage metrics.
type Statistics struct {
	InUse, MaxUse, InIdle, MaxIdle, Hits, Misses uint32
	PoolTooSmall, PoolCap                        uint32
}

// add adds d to c, and adjusts m as max.
func add(c *uint32, d uint32, m *uint32) {
	act := atomic.AddUint32(c, d)
	max := atomic.LoadUint32(m)
	if act > max {
		atomic.CompareAndSwapUint32(m, max, act)
	}
}

func (s Statistics) String() string {
	small := ""
	if s.PoolTooSmall > 0 {
		small = fmt.Sprintf(" The pool is too small with %d capacity.", s.PoolCap)
	}
	return fmt.Sprintf("Actually %d connections are in use, %d is idle. Max used %d, max idle was %d. There were %d hits and %d misses.", s.InUse, s.InIdle, s.MaxUse, s.MaxIdle, s.Hits, s.Misses) + small
}

type goConnectionPool struct {
	pool                    chan *Connection
	username, password, sid string
	stats                   Statistics
}

// NewGoConnectionPool returns a simple caching pool, with limited number of idle connections,
// but unbounded any other way.
func NewGoConnectionPool(username, password, sid string, maxIdle int) (ConnectionPool, error) {
	if maxIdle <= 0 {
		maxIdle = 999
	}
	return &goConnectionPool{
		pool:     make(chan *Connection, maxIdle),
		username: username, password: password, sid: sid,
		stats: Statistics{PoolCap: uint32(maxIdle)},
	}, nil
}

func (cp *goConnectionPool) Get() (*Connection, error) {
	add(&cp.stats.InUse, 1, &cp.stats.MaxUse)
	select {
	case c := <-cp.pool:
		atomic.AddUint32(&cp.stats.InIdle, ^uint32(0)) // decrement
		atomic.AddUint32(&cp.stats.Hits, 1)
		return c, nil
	default:
		atomic.AddUint32(&cp.stats.Misses, 1)
		c, err := NewConnection(cp.username, cp.password, cp.sid, false)
		if err != nil {
			return nil, err
		}
		c.connectionPool = cp
		return c, nil
	}
}

func (cp *goConnectionPool) Put(conn *Connection) {
	if !conn.IsConnected() {
		return
	}
	atomic.AddUint32(&cp.stats.InUse, ^uint32(0)) // decremenet
	select {
	case cp.pool <- conn:
		add(&cp.stats.InIdle, 1, &cp.stats.MaxIdle)
		// in chan
	default:
		atomic.AddUint32(&cp.stats.PoolTooSmall, 1)
		conn.connectionPool = nil
		conn.Close()
	}
}

func (cp *goConnectionPool) Close() error {
	if cp == nil || cp.pool == nil {
		return nil
	}
	close(cp.pool)
	for c := range cp.pool {
		c.connectionPool = nil
		c.Close()
	}
	cp.pool = nil
	return nil
}

func (cp goConnectionPool) Stats() Statistics {
	return cp.stats
}

// oraConnectionPool holds C.OCICPool for connection pooling
// - see http://docs.oracle.com/cd/E11882_01/appdev.112/e10646/oci09adv.htm#LNOCI16602
type oraConnectionPool struct {
	handle      *C.OCICPool
	authHandle  *C.OCIAuthInfo
	environment *Environment
	name        string
	stats       Statistics
	closedMtx   sync.Mutex
	closed      bool
}

// NewORAConnectionPool returns a new connection pool wrapping an OCI Connection Pool.
//
// It seems that it does not treat connMax as a hard upper bound! Although the number of
// actual physical connections (TCP sockets in use) IS bounded.
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
	if CTrace {
		ctrace("OCIConnectionPoolCreate(env=%p, err=%p, handle=%p, namep, namelen, dblink=%s, len(dblink)=%d connMin=%d, connMax=%d, connIncr=%d, username, password, OCI_DEFAULT)",
			env.handle, env.errorHandle, pool.handle,
			dblink, len(dblink),
			C.ub4(connMin), C.ub4(connMax), C.ub4(connIncr))

	}
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
	if CTrace {
		ctrace("pool.name=%q", pool.name)
	}

	it := C.ub4(IdleTimeout / time.Second)
	if err = env.AttrSet(unsafe.Pointer(pool.handle), C.OCI_HTYPE_CPOOL,
		C.OCI_ATTR_CONN_TIMEOUT, unsafe.Pointer(&it), 0,
	); err != nil {
		return nil, err
	}

	return &pool, nil
}

// Close the connection pool.
func (cp *oraConnectionPool) Close() error {
	cp.closedMtx.Lock()
	if cp.closed {
		cp.closedMtx.Unlock()
		return nil
	}
	cp.closed = true
	cp.closedMtx.Unlock()

	if cp.authHandle != nil {
		C.OCIHandleFree(unsafe.Pointer(cp.authHandle), C.OCI_HTYPE_AUTHINFO)
	}
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

func (cp *oraConnectionPool) IsClosed() bool {
	cp.closedMtx.Lock()
	closed := cp.closed || cp.handle == nil
	cp.closedMtx.Unlock()
	return closed
}

// Acquire a new connection.
// On Close of this returned connection, it will only released back to the pool.
func (cp *oraConnectionPool) Get() (*Connection, error) {
	if cp.IsClosed() {
		return nil, ErrPoolIsClosed
	}
	conn := &Connection{connectionPool: cp, environment: cp.environment}
	if CTrace {
		ctrace("OCISessionGet(env=%p, errHndl=%p, conn=%p, auth=%p, name=%q, nameLen=%d, ... OCI_SESSGET_CPOOL)", cp.environment.handle, cp.environment.errorHandle,
			&conn.handle, cp.authHandle,
			cp.name, C.ub4(len(cp.name)))
	}
	if err := cp.environment.CheckStatus(
		C.OCISessionGet(cp.environment.handle, cp.environment.errorHandle,
			&conn.handle, cp.authHandle,
			(*C.OraText)(unsafe.Pointer(&([]byte(cp.name))[0])), C.ub4(len(cp.name)),
			nil, 0, nil, nil, nil,
			C.OCI_SESSGET_CPOOL),
		"SessionGet"); err != nil {
		return nil, err
	}
	add(&cp.stats.InUse, 1, &cp.stats.MaxUse)
	return conn, nil
}

// Release a connection back to the pool.
func (cp *oraConnectionPool) Put(conn *Connection) {
	if conn == nil || conn.handle == nil || conn.connectionPool == nil || !conn.IsConnected() {
		return
	}
	add(&cp.stats.InIdle, 1, &cp.stats.MaxIdle)
	conn.srvMtx.Lock()
	defer conn.srvMtx.Unlock()
	err := cp.environment.CheckStatus(
		C.OCISessionRelease(conn.handle, cp.environment.errorHandle, nil, 0, C.OCI_DEFAULT),
		"SessionRelease")
	if err != nil {
		log.Printf("SessionRelease ERROR: %v", err)
		conn.connectionPool = nil
		conn.Close()
		conn.handle = nil
	}
	return
}

// Stats returns the Statistics of the pool.
func (cp *oraConnectionPool) Stats() Statistics {
	return cp.stats
}

// oraSessionPool holds C.OCISPool for session pooling
// - see http://docs.oracle.com/cd/E11882_01/appdev.112/e10646/oci09adv.htm#LNOCI16602
type oraSessionPool struct {
	handle      *C.OCISPool
	authHandle  *C.OCIAuthInfo
	environment *Environment
	name        string
	stats       Statistics
	closedMtx   sync.Mutex
	closed      bool
}

// NewORASessionPool returns a new session pool wrapping an OCI Session Pool.
// - http://docs.oracle.com/cd/E11882_01/appdev.112/e10646/oci16rel001.htm#LNOCI17124
//
// NOT WORKING ATM.
func NewORASessionPool(username, password, dblink string, connMin, connMax, connIncr int, homogeneous bool) (ConnectionPool, error) {
	env, err := NewEnvironment()
	if err != nil {
		return nil, err
	}
	var pool oraSessionPool
	if err = ociHandleAlloc(unsafe.Pointer(env.handle),
		C.OCI_HTYPE_SPOOL, (*unsafe.Pointer)(unsafe.Pointer(&pool.handle)),
		"pool.handle"); err != nil || pool.handle == nil {
		return nil, err
	}

	if err = ociHandleAlloc(unsafe.Pointer(env.handle),
		C.OCI_HTYPE_AUTHINFO, (*unsafe.Pointer)(unsafe.Pointer(&pool.authHandle)),
		"pool.authHandle"); err != nil || pool.authHandle == nil {
		C.OCIHandleFree(unsafe.Pointer(pool.handle), C.OCI_HTYPE_SPOOL)
		return nil, err
	}
	defer func() {
		if err != nil {
			if pool.authHandle != nil {
				C.OCIHandleFree(unsafe.Pointer(pool.authHandle), C.OCI_HTYPE_AUTHINFO)
			}
			if pool.handle != nil {
				C.OCIHandleFree(unsafe.Pointer(pool.handle), C.OCI_HTYPE_SPOOL)
			}
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

	mode := C.ub4(C.OCI_DEFAULT | C.OCI_SPC_STMTCACHE) // STMTCACHE -> garble
	//mode := C.ub4(C.OCI_DEFAULT)
	if homogeneous {
		mode |= C.OCI_SPC_HOMOGENEOUS
	}
	if CTrace {
		ctrace("OCISessionPoolCreate(env=%p, err=%p, handle=%p, namep, namelen, dblink=%s, len(dblink)=%d connMin=%d, connMax=%d, connIncr=%d, username, password, mode=%d)",
			env.handle, env.errorHandle, pool.handle,
			dblink, len(dblink),
			C.ub4(connMin), C.ub4(connMax), C.ub4(connIncr), mode)

	}
	var (
		nameP   unsafe.Pointer
		nameLen C.ub4
	)
	if err = env.CheckStatus(
		C.OCISessionPoolCreate(env.handle, env.errorHandle, pool.handle,
			(**C.OraText)(unsafe.Pointer(&nameP)), &nameLen,
			(*C.OraText)(unsafe.Pointer(&([]byte(dblink)[0]))), C.ub4(len(dblink)),
			C.ub4(connMin), C.ub4(connMax), C.ub4(connIncr),
			(*C.OraText)(unsafe.Pointer(&([]byte(username)[0]))), C.ub4(len(username)),
			(*C.OraText)(unsafe.Pointer(&([]byte(password)[0]))), C.ub4(len(password)),
			mode),
		"CreateSessionPool"); err != nil {
		return nil, err
	}
	pool.name = C.GoStringN((*C.char)(nameP), C.int(nameLen))
	pool.environment = env
	if CTrace {
		ctrace("pool.name=%q", pool.name)
	}
	if homogeneous {
		C.OCIHandleFree(unsafe.Pointer(pool.authHandle), C.OCI_HTYPE_AUTHINFO)
		pool.authHandle = nil
	}

	it := C.ub4(IdleTimeout / time.Second)
	if err = env.AttrSet(unsafe.Pointer(pool.handle), C.OCI_HTYPE_SPOOL,
		C.OCI_ATTR_CONN_TIMEOUT, unsafe.Pointer(&it), 0,
	); err != nil {
		//return nil, err
	}

	return &pool, nil
}

// Close the connection pool.
func (cp *oraSessionPool) Close() error {
	cp.closedMtx.Lock()
	if cp.closed || cp.handle == nil {
		cp.closedMtx.Unlock()
		return nil
	}
	cp.closed = true
	if cp.authHandle != nil {
		C.OCIHandleFree(unsafe.Pointer(cp.authHandle), C.OCI_HTYPE_AUTHINFO)
	}
	cp.authHandle = nil
	cp.closedMtx.Unlock()

	if cp.handle == nil {
		return nil
	}
	err := cp.environment.CheckStatus(
		C.OCISessionPoolDestroy(cp.handle, cp.environment.errorHandle, C.OCI_DEFAULT),
		"SessionPoolDestroy")
	C.OCIHandleFree(unsafe.Pointer(cp.handle), C.OCI_HTYPE_SPOOL)
	cp.handle = nil
	return err
}

func (cp *oraSessionPool) IsClosed() bool {
	cp.closedMtx.Lock()
	closed := cp.closed || cp.handle == nil
	cp.closedMtx.Unlock()
	return closed
}

// Acquire a new connection.
// On Close of this returned connection, it will only released back to the pool.
func (cp *oraSessionPool) Get() (*Connection, error) {
	if cp.IsClosed() {
		return nil, ErrPoolIsClosed
	}
	cp.closedMtx.Lock()
	authHandle := cp.authHandle
	cp.closedMtx.Unlock()

	conn := &Connection{connectionPool: cp, environment: cp.environment}
	if CTrace {
		ctrace("OCISessionGet(env=%p, errHndl=%p, conn=%p, auth=%p, name=%q, nameLen=%d, ... OCI_SESSGET_SPOOL)", cp.environment.handle, cp.environment.errorHandle,
			&conn.handle, authHandle,
			cp.name, C.ub4(len(cp.name)))
	}
	if err := cp.environment.CheckStatus(
		C.OCISessionGet(cp.environment.handle, cp.environment.errorHandle,
			&conn.handle, authHandle,
			(*C.OraText)(unsafe.Pointer(&([]byte(cp.name))[0])), C.ub4(len(cp.name)),
			nil, 0, nil, nil, nil,
			C.OCI_SESSGET_SPOOL),
		"SessionGet"); err != nil {
		return nil, err
	}
	add(&cp.stats.InUse, 1, &cp.stats.MaxUse)
	return conn, nil
}

// Release a connection back to the pool.
func (cp *oraSessionPool) Put(conn *Connection) {
	if conn == nil || conn.handle == nil || conn.connectionPool == nil || !conn.IsConnected() {
		return
	}
	add(&cp.stats.InIdle, 1, &cp.stats.MaxIdle)
	conn.srvMtx.Lock()
	defer conn.srvMtx.Unlock()
	err := cp.environment.CheckStatus(
		C.OCISessionRelease(conn.handle, cp.environment.errorHandle, nil, 0, C.OCI_DEFAULT),
		"SessionRelease")
	if err != nil {
		log.Printf("SessionRelease ERROR: %v", err)
		conn.connectionPool = nil
		conn.Close()
		conn.handle = nil
	}
	return
}

// Stats returns the Statistics of the pool.
func (cp *oraSessionPool) Stats() Statistics {
	return cp.stats
}
