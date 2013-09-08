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

#include <stdlib.h>
#include <oci.h>
//#include <string.h>
//#include <xa.h>

sword setCallback(dvoid *handle, OCIError *errhp) {
    return OCIAttrSet(handle, OCI_HTYPE_SUBSCRIPTION,
				(dvoid*)callback, 0, OCI_ATTR_SUBSCR_CALLBACK,
				errhp);
}
*/
import "C"

import (
	"errors"
	"fmt"
	"log"
	"time"
	"unsafe"
)

type subscription struct {
	handle                                                 *C.OCISubscription
	connection                                             *Connection
	namespace, protocol, port, timeout, operations, rowids C.ub4
	happened                                               chan<- *Message
}

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
		rowidLength C.ub4
		rowid       *C.char
	)

	// determine operation
	if err = env.CheckStatus(
		C.OCIAttrGet(descriptor, C.OCI_DTYPE_ROW_CHDES,
			unsafe.Pointer(&mr.Operation), nil,
			C.OCI_ATTR_CHDES_ROW_OPFLAGS, env.errorHandle),
		"MessageRow_Initialize: get operation"); err != nil {
		return err
	}

	// determine table name
	if err = env.CheckStatus(
		C.OCIAttrGet(descriptor, C.OCI_DTYPE_ROW_CHDES,
			unsafe.Pointer(&rowid), &rowidLength,
			C.OCI_ATTR_CHDES_ROW_ROWID, env.errorHandle),
		"MessageRow_Initialize(): get rowid"); err != nil {
		return err
	}
	if rowid == nil {
		return errors.New("MessageRow_Initialize(): nil rowid")
	}
	mr.Rowid = env.FromEncodedBytes(rowid, rowidLength)
	return nil
}

// Initialize a new message table with the information from the descriptor.
func (mt *MessageTable) Initialize(env *Environment, descriptor unsafe.Pointer) error {
	var (
		err           error
		nameLength    C.ub4
		name          *C.char
		rows          *C.OCIColl
		numRows       C.sb4
		exists        C.boolean
		rowDescriptor **C.dvoid
		indicator     *C.dvoid
	)

	// determine operation
	if err = env.CheckStatus(
		C.OCIAttrGet(descriptor, C.OCI_DTYPE_TABLE_CHDES,
			unsafe.Pointer(&mt.Operation), nil,
			C.OCI_ATTR_CHDES_TABLE_OPFLAGS, env.errorHandle),
		"MessageTable_Initialize(): get operation"); err != nil {
		return err
	}

	// determine table name
	if err = env.CheckStatus(
		C.OCIAttrGet(descriptor, C.OCI_DTYPE_TABLE_CHDES,
			unsafe.Pointer(&name), &nameLength,
			C.OCI_ATTR_CHDES_TABLE_NAME, env.errorHandle),
		"MessageTable_Initialize(): get table name"); err != nil {
		return err
	}
	if name == nil {
		return errors.New("MessageTable_Initialize(): empty table name")
	}
	mt.Name = env.FromEncodedBytes(name, nameLength)

	// if change invalidated all rows, nothing to do
	if mt.Operation&C.OCI_OPCODE_ALLROWS > 0 {
		return nil
	}

	// determine rows collection
	if err = env.CheckStatus(
		C.OCIAttrGet(descriptor, C.OCI_DTYPE_TABLE_CHDES,
			unsafe.Pointer(&rows), nil,
			C.OCI_ATTR_CHDES_TABLE_ROW_CHANGES, env.errorHandle),
		"MessageTable_Initialize(): get rows collection"); err != nil {
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
		dbnameLength    C.ub4
		tables          *C.OCIColl
		numTables       C.sb4
		exists          C.boolean
		tableDescriptor **C.dvoid
		indicator       *C.dvoid
	)

	// determine type
	if err = env.CheckStatus(
		C.OCIAttrGet(descriptor, C.OCI_DTYPE_CHDES,
			unsafe.Pointer(&m.Type), nil,
			C.OCI_ATTR_CHDES_NFYTYPE, env.errorHandle),
		"Message_Initialize(): get type"); err != nil {
		return err
	}

	// determine database name
	if err = env.CheckStatus(
		C.OCIAttrGet(descriptor, C.OCI_DTYPE_CHDES,
			unsafe.Pointer(&dbname), &dbnameLength,
			C.OCI_ATTR_CHDES_DBNAME, env.errorHandle),
		"Message_Initialize(): get database name"); err != nil {
		return err
	}
	if dbname == nil {
		return errors.New("Message_Initialize(): empty dbname")
	}
	m.DBName = env.FromEncodedBytes(dbname, dbnameLength)

	// determine table collection
	if err = env.CheckStatus(
		C.OCIAttrGet(descriptor, C.OCI_DTYPE_CHDES,
			unsafe.Pointer(&tables), nil,
			C.OCI_ATTR_CHDES_TABLE_CHANGES, env.errorHandle),
		"Message_Initialize(): get tables collection"); err != nil {
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
	// create the message
	m := new(Message)
	if err := m.Initialize(env, descriptor); err != nil {
		return err
	}

	s.happened <- m

	return nil
}

// Callback is the routine that is called when a callback needs to be invoked.
//notification_callback (dvoid *ctx, OCISubscription *subscrhp, dvoid *payload, ub4 paylen, dvoid *desc, ub4 mode)
//export callback
func callback(ctx unsafe.Pointer, handle unsafe.Pointer, payload unsafe.Pointer, payloadLength C.ub4, descriptor unsafe.Pointer, mode C.ub4) C.ub4 {
	env, err := NewEnvironment()
	if err != nil {
		log.Printf("ERROR creating new environment in subscription callback: %s", err)
		return 0
	}

	s := (*subscription)(unsafe.Pointer(ctx))
	if err = s.callbackHandler(env, descriptor); err != nil {
		return 0
	}
	return 1
}

// Register the subscription.
func (s *subscription) Register() error {
	var err error

	// create the subscription handle
	env := s.connection.environment
	if err = env.CheckStatus(
		C.OCIHandleAlloc(unsafe.Pointer(env.handle),
			(*unsafe.Pointer)(unsafe.Pointer(&s.handle)),
			C.OCI_HTYPE_SUBSCRIPTION, 0, nil),
		"Subscription_Register(): allocate handle"); err != nil {
		return err
	}

	// set the namespace
	if err = env.CheckStatus(
		C.OCIAttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
			unsafe.Pointer(&s.namespace), C.sizeof_ub4,
			C.OCI_ATTR_SUBSCR_NAMESPACE,
			env.errorHandle),
		"Subscription_Register(): set namespace"); err != nil {
		return err
	}

	// set the protocol
	if err = env.CheckStatus(
		C.OCIAttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
			unsafe.Pointer(&s.protocol), C.sizeof_ub4,
			C.OCI_ATTR_SUBSCR_RECPTPROTO,
			env.errorHandle),
		"Subscription_Register(): set protocol"); err != nil {
		return err
	}

	// set the timeout
	if err = env.CheckStatus(
		C.OCIAttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
			unsafe.Pointer(&s.timeout), C.sizeof_ub4,
			C.OCI_ATTR_SUBSCR_TIMEOUT,
			env.errorHandle),
		"Subscription_Register(): set timeout"); err != nil {
		return err
	}

	// set the TCP port used on client to listen for callback from DB server
	if s.port > 0 {
		if err = env.CheckStatus(
			C.OCIAttrSet(unsafe.Pointer(env.handle), C.OCI_HTYPE_ENV,
				unsafe.Pointer(&s.port), C.ub4(0), C.OCI_ATTR_SUBSCR_PORTNO,
				env.errorHandle),
			"Subscription_Register(): set port"); err != nil {
			return err
		}
	}

	// set the context for the callback
	if err = env.CheckStatus(
		C.OCIAttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
			unsafe.Pointer(s), 0, C.OCI_ATTR_SUBSCR_CTX, env.errorHandle),
		"Subscription_Register(): set context"); err != nil {
		return err
	}

	// set the callback, if applicable
	if s.happened != nil {
		if err = env.CheckStatus(
			C.setCallback(unsafe.Pointer(s.handle), env.errorHandle),
			"Subscription_Register(): set callback"); err != nil {
			return err
		}
	}

	// set whether or not rowids are desired
	if err = env.CheckStatus(
		C.OCIAttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
			unsafe.Pointer(&s.rowids), C.sizeof_ub4, C.OCI_ATTR_CHNF_ROWIDS,
			env.errorHandle),
		"Subscription_Register(): set rowids"); err != nil {
		return err
	}

	// set which operations are desired
	if err = env.CheckStatus(
		C.OCIAttrSet(unsafe.Pointer(s.handle), C.OCI_HTYPE_SUBSCRIPTION,
			unsafe.Pointer(&s.operations), C.sizeof_ub4, C.OCI_ATTR_CHNF_OPERATIONS,
			env.errorHandle),
		"Subscription_Register(): set operations"); err != nil {
		return err
	}

	// register the subscription
	//Py_BEGIN_ALLOW_THREADS
	if err = env.CheckStatus(
		C.OCISubscriptionRegister(s.connection.handle,
			&s.handle, 1, env.errorHandle, C.OCI_DEFAULT),
		//Py_END_ALLOW_THREADS
		"Subscription_Register(): register"); err != nil {
		return err
	}

	return nil
}

// NewSubscription allocates a new subscription object.
func NewSubscription(connection *Connection, namespace, protocol, port uint, timeout time.Duration, operations uint, rowids bool, happened chan<- *Message) (*subscription, error) {
	s := &subscription{connection: connection, namespace: C.ub4(namespace),
		protocol: C.ub4(protocol), port: C.ub4(port), operations: C.ub4(operations),
		timeout:  C.ub4(timeout.Seconds()),
		happened: happened}
	if rowids {
		s.rowids = 1
	}

	return s, s.Register()
}

// Free the memory associated with a subscription.
func (s *subscription) Free() {
	if s.handle != nil {
		C.OCISubscriptionUnRegister(s.connection.handle,
			s.handle, s.connection.environment.errorHandle,
			C.OCI_DEFAULT)
	}
	s.connection = nil
	s.happened = nil
}

// String returns a string representation of the subscription.
func (s *subscription) String() string {
	return fmt.Sprintf("<subscription on %s>", s.connection)
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
	if err = env.CheckStatus(
		C.OCIAttrSet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
			unsafe.Pointer(s.handle), 0,
			C.OCI_ATTR_CHNF_REGHANDLE, env.errorHandle),
		"Subscription_RegisterQuery(): set subscription handle"); err != nil {
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
