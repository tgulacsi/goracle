// +build !nosubscription

package oracle

/*
Copyright 2013 Tamás Gulácsi

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

/*
#cgo LDFLAGS: -lclntsh

#include <oci.h>
#include "subscription_cb.h"
*/
import "C"

import (
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
	"unsafe"
)

// ModeSubscription is the required open mode for subscriptions to work
const ModeSubscription = C.OCI_EVENTS | C.OCI_OBJECT

type subscription struct {
	handle                                         *C.OCISubscription
	connection                                     *Connection
	name                                           []byte
	namespace, protocol, port, timeout, operations C.ub4
	rowids, qosReliable                            bool
	callbackID                                     uint8
	happened                                       chan<- *Message
}

var callbacksMu sync.Mutex
var callbacks map[uint8]*subscription

// Message is the subscription message
type Message struct {
	// Type is the change's type
	Type C.ub4
	// DBName is the database's name
	DBName string
	// Tables represents the tables which has changed
	Tables []MessageTable
}

// MessageTable is a table which has changed
type MessageTable struct {
	// Name is the name of the table
	Name string
	// Operation is the change's type
	Operation C.ub4
	// Rows represents the rows which has changed
	Rows []MessageRow
}

// MessageRow is a row which has changed
type MessageRow struct {
	// Rowid is the row's ID which has changed
	Rowid string
	// Operation is the change's type
	Operation C.ub4
}

// Initialize a new message row with the information from the descriptor.
func (mr *MessageRow) Initialize(env *Environment, descriptor unsafe.Pointer) error {
	var (
		err         error
		rowidLength int
		rowid       *C.char
	)

	// determine operation
	if _, err := env.AttrGet(descriptor, C.OCI_DTYPE_ROW_CHDES,
		C.OCI_ATTR_CHDES_ROW_OPFLAGS, unsafe.Pointer(&mr.Operation),
		"MessageRow_Initialize: get operation",
	); err != nil {
		return err
	}

	// determine table name
	if rowidLength, err = env.AttrGet(descriptor, C.OCI_DTYPE_ROW_CHDES,
		C.OCI_ATTR_CHDES_ROW_ROWID, unsafe.Pointer(&rowid),
		"MessageRow_Initialize(): get rowid",
	); err != nil {
		return err
	}
	if rowid == nil {
		return errors.New("MessageRow_Initialize(): nil rowid")
	}
	mr.Rowid = env.FromEncodedBytes(rowid, C.ub4(rowidLength))
	return nil
}

// Initialize a new message table with the information from the descriptor.
func (mt *MessageTable) Initialize(env *Environment, descriptor unsafe.Pointer) error {
	var (
		err           error
		nameLength    int
		name          *C.char
		rows          *C.OCIColl
		numRows       C.sb4
		exists        C.boolean
		rowDescriptor **C.dvoid
		indicator     *C.dvoid
	)

	// determine operation
	if _, err = env.AttrGet(descriptor, C.OCI_DTYPE_TABLE_CHDES,
		C.OCI_ATTR_CHDES_TABLE_OPFLAGS, unsafe.Pointer(&mt.Operation),
		"MessageTable_Initialize(): get operation",
	); err != nil {
		return err
	}

	// determine table name
	if nameLength, err = env.AttrGet(descriptor, C.OCI_DTYPE_TABLE_CHDES,
		C.OCI_ATTR_CHDES_TABLE_NAME, unsafe.Pointer(&name),
		"MessageTable_Initialize(): get table name",
	); err != nil {
		return err
	}
	if name == nil {
		return errors.New("MessageTable_Initialize(): empty table name")
	}
	mt.Name = env.FromEncodedBytes(name, C.ub4(nameLength))

	// if change invalidated all rows, nothing to do
	if mt.Operation&C.OCI_OPCODE_ALLROWS > 0 {
		return nil
	}

	// determine rows collection
	if _, err = env.AttrGet(descriptor, C.OCI_DTYPE_TABLE_CHDES,
		C.OCI_ATTR_CHDES_TABLE_ROW_CHANGES, unsafe.Pointer(&rows),
		"MessageTable_Initialize(): get rows collection",
	); err != nil {
		return err
	}

	// determine number of rows in collection
	if err = env.CheckStatus(
		C.OCICollSize(env.handle, env.errorHandle, rows, &numRows),
		"MessageTable_Initialize(): get size of rows collection"); err != nil {
		return err
	}

	// populate the rows attribute
	mt.Rows = make([]MessageRow, numRows)
	for i := 0; i < int(numRows); i++ {
		if err = env.CheckStatus(
			C.OCICollGetElem(env.handle, env.errorHandle, rows, C.sb4(i),
				&exists, (*unsafe.Pointer)(unsafe.Pointer(&rowDescriptor)),
				(*unsafe.Pointer)(unsafe.Pointer(&indicator))),
			"MessageTable_Initialize(): get element from collection"); err != nil {
			return err
		}
		if err = mt.Rows[i].Initialize(env, unsafe.Pointer(*rowDescriptor)); err != nil {
			return err
		}
	}

	return nil
}

// Initialize a new message with the information from the descriptor.
func (m *Message) Initialize(env *Environment, descriptor unsafe.Pointer) error {
	var (
		err             error
		dbname          *C.char
		dbnameLength    int
		tables          *C.OCIColl
		numTables       C.sb4
		exists          C.boolean
		tableDescriptor **C.dvoid
		indicator       *C.dvoid
	)

	// determine type
	if _, err = env.AttrGet(descriptor, C.OCI_DTYPE_CHDES,
		C.OCI_ATTR_CHDES_NFYTYPE, unsafe.Pointer(&m.Type),
		"Message_Initialize(): get type",
	); err != nil {
		return err
	}

	// determine database name
	if dbnameLength, err = env.AttrGet(descriptor, C.OCI_DTYPE_CHDES,
		C.OCI_ATTR_CHDES_DBNAME, unsafe.Pointer(&dbname),
		"Message_Initialize(): get database name",
	); err != nil {
		return err
	}
	if dbname == nil {
		return errors.New("Message_Initialize(): empty dbname")
	}
	m.DBName = env.FromEncodedBytes(dbname, C.ub4(dbnameLength))

	// determine table collection
	if _, err = env.AttrGet(descriptor, C.OCI_DTYPE_CHDES,
		C.OCI_ATTR_CHDES_TABLE_CHANGES, unsafe.Pointer(&tables),
		"Message_Initialize(): get tables collection",
	); err != nil {
		return err
	}

	// determine number of tables
	if tables == nil {
		numTables = 0
	} else {
		if err = env.CheckStatus(
			C.OCICollSize(env.handle, env.errorHandle, tables, &numTables),
			"Message_Initialize(): get size of collection"); err != nil {
			return err
		}
	}

	// create list to hold results
	m.Tables = make([]MessageTable, numTables)

	// populate each entry with a message table instance
	for i := 0; i < int(numTables); i++ {
		if err = env.CheckStatus(
			C.OCICollGetElem(env.handle, env.errorHandle, tables, C.sb4(i),
				&exists, (*unsafe.Pointer)(unsafe.Pointer(&tableDescriptor)),
				(*unsafe.Pointer)(unsafe.Pointer(&indicator))),
			"Message_Initialize(): get element from collection"); err != nil {
			return err
		}
		if err = m.Tables[i].Initialize(env, unsafe.Pointer(*tableDescriptor)); err != nil {
			return err
		}
	}
	return nil
}

// callbackHandler is the routine that performs the actual call.
func (s subscription) callbackHandler(env *Environment, descriptor unsafe.Pointer) error {
	log.Printf("callbackHandler(%p, %p)", env, descriptor)
	// create the message
	m := new(Message)
	if err := m.Initialize(env, descriptor); err != nil {
		return err
	}

	s.happened <- m

	return nil
}

// Register the subscription.
func (s *subscription) Register() error {
	var err error

	//
	// WARNING! for this the environment MUST BE opened with OCI_EVENTS mode!
	//
	// https://www.stanford.edu/dept/itss/docs/oracle/10gR2/appdev.102/b14251/adfns_dcn.htm#BDCEJDDG
	// the user is required to have the CHANGE NOTIFICATION system privilege. In addition the user is required to have SELECT privileges on all objects to be registered

	// create the subscription handle
	env := s.connection.environment
	if ociHandleAlloc(unsafe.Pointer(env.handle), C.OCI_HTYPE_SUBSCRIPTION,
		(*unsafe.Pointer)(unsafe.Pointer(&s.handle)),
		"Subscription.Register[allocate handle]",
	); err != nil {
		return err
	}

	// set the TCP port used on client to listen for callback from DB server
	if s.port > 0 {
		if err = env.AttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
			C.OCI_ATTR_SUBSCR_PORTNO,
			unsafe.Pointer(&s.port), 0,
			"Subscription.Register[set port]",
		); err != nil {
			return err
		}
	}

	// set the timeout
	if err = env.AttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
		C.OCI_ATTR_SUBSCR_TIMEOUT,
		unsafe.Pointer(&s.timeout), C.sizeof_ub4,
		"Subscription.Register[set timeout]",
	); err != nil {
		return err
	}

	// set the name
	if err = env.AttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
		C.OCI_ATTR_SUBSCR_NAME,
		unsafe.Pointer(&s.name[0]), len(s.name),
		"Subscription.Register[set namespace]",
	); err != nil {
		return err
	}

	// set the namespace
	if s.namespace != C.OCI_SUBSCR_NAMESPACE_DBCHANGE {
		log.Printf("subscription namespace is %d, not DBCHANGE!", s.namespace)
	}
	if err = env.AttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
		C.OCI_ATTR_SUBSCR_NAMESPACE,
		unsafe.Pointer(&s.namespace), C.sizeof_ub4,
		"Subscription.Register[set namespace]",
	); err != nil {
		return err
	}

	// set the protocol
	if s.protocol != C.OCI_SUBSCR_PROTO_OCI {
		log.Printf("subscription protocol is %d, not OCI!", s.protocol)
	}
	if err = env.AttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
		C.OCI_ATTR_SUBSCR_RECPTPROTO,
		unsafe.Pointer(&s.protocol), C.sizeof_ub4,
		"Subscription.Register[set protocol]",
	); err != nil {
		return err
	}

	// set the callback, if applicable
	if s.happened != nil {
		log.Println("subscription: setting callback")
		if err = env.AttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
			C.OCI_ATTR_SUBSCR_CALLBACK,
			unsafe.Pointer(&C.goCallback), 0,
			"Subscription.Register[set callback]",
		); err != nil {
			return err
		}
		/*
			if err = env.CheckStatus(
				C.setSubsCallback(s.handle, env.errorHandle),
				"Subscription_Register(): setSubsCallback"); err != nil {
				return fmt.Errorf("error: %v", err)
			}
		*/

	}

	// set whether or not rowids are desired
	rowids := C.ub4(0)
	if s.rowids {
		rowids = 1
	}
	if err = env.AttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
		C.OCI_ATTR_CHNF_ROWIDS,
		unsafe.Pointer(&rowids), C.sizeof_ub4,
		"Subscription.Register[set rowids]",
	); err != nil {
		return err
	}

	callbacksMu.Lock()
	if callbacks == nil {
		callbacks = make(map[uint8]*subscription, 1)
	}
	s.callbackID = uint8(len(callbacks))
	callbacks[s.callbackID] = s
	callbacksMu.Unlock()

	// set the context for the callback
	if err = env.AttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
		C.OCI_ATTR_SUBSCR_CTX,
		unsafe.Pointer(&s.callbackID), C.sizeof_void,
		"Subscription.Register[set context]",
	); err != nil {
		return err
	}

	// set which operations are desired
	if err = env.AttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
		C.OCI_ATTR_CHNF_OPERATIONS, unsafe.Pointer(&s.operations),
		C.sizeof_ub4,
		"Subscription.Register[set operations]",
	); err != nil {
		return err
	}

	// set notification reliability
	qos := C.ub4(0)
	if s.qosReliable {
		qos++
	}
	if err = env.AttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
		C.OCI_ATTR_SUBSCR_QOSFLAGS, unsafe.Pointer(&qos), C.sizeof_ub4,
		"Subscription.Register[set qos reliability]",
	); err != nil {
		return err
	}

	// register the subscription
	//Py_BEGIN_ALLOW_THREADS
	if err = env.CheckStatus(
		C.OCISubscriptionRegister(s.connection.handle,
			&s.handle, 1, env.errorHandle, C.OCI_DEFAULT),
		//Py_END_ALLOW_THREADS
		"Subscription.Register[register]"); err != nil {
		return err
	}

	return nil
}

// NewSubscription allocates a new subscription object.
func NewSubscription(connection *Connection, name string,
	namespace, protocol, port uint,
	timeout time.Duration, operations uint,
	rowids bool, happened chan<- *Message) (*subscription, error) {

	if namespace <= 0 {
		namespace = C.OCI_SUBSCR_NAMESPACE_DBCHANGE
	}
	if protocol <= 0 {
		protocol = C.OCI_SUBSCR_PROTO_OCI
	}
	if operations <= 0 {
		operations = C.OCI_OPCODE_INSERT | C.OCI_OPCODE_DELETE | C.OCI_OPCODE_UPDATE
	}
	var nameB []byte
	if name != "" {
		nameB = []byte(name)
	} else {
		nameB = make([]byte, 16)
		n, err := rand.Read(nameB)
		if err != nil {
			return nil, err
		}
		if n < 16 {
			nameB = nameB[:n]
		}
	}
	s := &subscription{connection: connection, namespace: C.ub4(namespace),
		protocol: C.ub4(protocol), port: C.ub4(port), operations: C.ub4(operations),
		name:   nameB,
		rowids: rowids, timeout: C.ub4(timeout.Seconds()),
		happened: happened}

	return s, s.Register()
}

// NewOCISubscription allocates a new subscription for OCI notification protocol
func NewOCISubscription(connection *Connection, name string, timeout time.Duration, operations uint, rowids bool, happened chan<- *Message) (*subscription, error) {
	return NewSubscription(connection, name,
		C.OCI_SUBSCR_NAMESPACE_DBCHANGE, C.OCI_SUBSCR_PROTO_OCI, 0,
		timeout, operations, rowids, happened)
}

// Free the memory associated with a subscription.
func (s *subscription) Free() {
	if s.handle != nil {
		C.OCISubscriptionUnRegister(s.connection.handle,
			s.handle, s.connection.environment.errorHandle,
			C.OCI_DEFAULT)
		C.OCIHandleFree(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION)
		s.handle = nil
	}
	s.connection = nil
	s.happened = nil
}

func (s *subscription) Close() error {
	s.Free()
	return nil
}

// String returns a string representation of the subscription.
func (s *subscription) String() string {
	return fmt.Sprintf("<subscription on %v>", s.connection)
}

// Register a query for database change notification.
func (s *subscription) RegisterQuery(qry string,
	listArgs []interface{}, keywordArgs map[string]interface{}) error {

	var err error
	env := s.connection.environment

	// create cursor to perform query
	cur := s.connection.NewCursor()

	// allocate the handle so the subscription handle can be set
	if err = cur.allocateHandle(); err != nil {
		return err
	}

	// prepare the statement for execution
	if err = cur.internalPrepare(qry, ""); err != nil {
		return err
	}

	// perform binds
	if listArgs != nil && len(listArgs) > 0 {
		if err = cur.setBindVariablesByPos(listArgs, 1, 0, false); err != nil {
			return err
		}
	} else if keywordArgs != nil && len(keywordArgs) > 0 {
		if err = cur.setBindVariablesByName(keywordArgs, 1, 0, false); err != nil {
			return err
		}
	}
	if err = cur.performBind(); err != nil {
		return err
	}

	// parse the query in order to get the defined variables
	//Py_BEGIN_ALLOW_THREADS
	if err = env.CheckStatus(
		C.OCIStmtExecute(s.connection.handle, cur.handle,
			env.errorHandle, 0, 0, nil, nil, C.OCI_DESCRIBE_ONLY),
		//Py_END_ALLOW_THREADS
		"Subscription_RegisterQuery(): parse statement"); err != nil {
		return err
	}

	// perform define as needed
	if cur.performDefine(); err != nil {
		return err
	}

	// set the subscription handle
	if s.handle == nil {
		return fmt.Errorf("Subscription_RegisterQuery(): subscription handle is nil!")
	}
	if err = env.AttrSet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
		C.OCI_ATTR_CHNF_REGHANDLE,
		unsafe.Pointer(s.handle), 0,
		"Subscription.RegisterQuery[set subscription handle]",
	); err != nil {
		return err
	}

	// execute the query which registers it
	if cur.internalExecute(0); err != nil {
		return err
	}

	return nil
}

// Free the memory associated with a message.
func (m *Message) Free() {
	m.DBName = ""
	m.Tables = nil
}

// Free the memory associated with a table in a message.
func (mt *MessageTable) Free() {
	mt.Name = ""
}

// Free the memory associated with a row in a message.
func (mr *MessageRow) Free() {
	mr.Rowid = ""
}
