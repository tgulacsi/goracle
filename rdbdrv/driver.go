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

// Package rdbdrv implements a Go Oracle driver for bitbucket.org/kardianos/rdb
package rdbdrv

import (
	"errors"

	"bitbucket.org/kardianos/rdb"
	"github.com/tgulacsi/goracle/oracle"
)

// Interface implementation checks.
var (
	_ rdb.Driver = &driver{}
	_ rdb.DriverConn = &conn{}
)

var (
	driverInfo = rdb.DriverInfo{
		DriverSupport: rdb.DriverSupport{
			PreparePerConn: true,
			NamedParameter: true,
			MultipleResult: true,
			Notification:   true,
		},
	}

	pingCommand = &rdb.Command{Sql: "SELECT 1 FROM DUAL", Arity: rdb.One, Prepare: true, Name: "ping"}
)

type driver struct {
}

// Open a database.
func (d *driver) Open(c *rdb.Config) (rdb.DriverConn, error) {
	if c.Secure {
		return nil, errors.New("Secure connection is not supported.")
	}

	// Establish the connection
	cx, err := oracle.NewConnection(c.Username, c.Password, c.Instance, false)
	if err != nil {
		return nil, err
	}
	return &conn{cx: cx}, nil
}

// Return information about the database driver's capabilities.
// Connection-independent.
func (d driver) DriverInfo() *rdb.DriverInfo {
	return &driverInfo
}

// Return the command to send a NOOP to the server.
func (d driver) PingCommand() *rdb.Command {
	return pingCommand
}

type conn struct {
	cx *oracle.Connection
}

// Close the underlying connection to the database.
func (c *conn) Close() {
	if c.cx == nil {
		return
	}
		c.cx.Close()
		c.cx = nil
	}

// Connectioninfo returns version information regarding the currently connected server.
func (c conn) ConnectionInfo() *ConnectionInfo {
}

// Scan reads the next row from the connection.
func (c conn) 
