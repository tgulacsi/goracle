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
package oracle

import (
	"testing"
	"time"
)

func TestSubscription(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	defer conn.Close()
	happened := make(chan *Message, 1)
	subs, err := NewOCISubscription(&conn, "", 5*time.Second, 0, false, happened)
	if err != nil {
		t.Errorf("NewOCISubscription: %v", subs)
		t.FailNow()
	}
	defer subs.Close()

	cur := conn.NewCursor()
	defer cur.Close()
	_ = cur.Execute("DROP TABLE TST_table", nil, nil)
	if err = cur.Execute("CREATE TABLE TST_table (key VARCHAR2(10), value VARCHAR2(100))",
		nil, nil); err != nil {

		t.Errorf("error creating TST_table: %v", err)
		t.FailNow()
	}
	defer cur.Execute("DROP TABLE TST_table", nil, nil)

	if err = subs.RegisterQuery("SELECT key FROM TST_table", nil, nil); err != nil {
		t.Errorf("error registering query SELECT key FROM TST_table: %v", err)
		t.FailNow()
	}

	if err = cur.Execute("INSERT INTO TST_table (key, value) VALUES('a', 'b')", nil, nil); err != nil {
		t.Errorf("error inserting into TST_table: %v", err)
	}
	if err = conn.Commit(); err != nil {
		t.Errorf("error commiting: %v", err)
		t.FailNow()
	}
	t.Logf("COMMIT - waiting for notification")
	select {
	case <-time.After(1 * time.Second):
		t.Errorf("notification timeout")
	case m := <-happened:
		t.Logf("got message %#v", m)
	}
}
