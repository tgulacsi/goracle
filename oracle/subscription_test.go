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
	subs, err := NewOCISubscription(&conn, 5*time.Second, 0, false, happened)
	if err != nil {
		t.Errorf("NewOCISubscription: %v", subs)
		t.FailNow()
	}
	defer subs.Close()

	if err = subs.RegisterQuery("SELECT * FROM cat", nil, nil); err != nil {
		t.Errorf("error registering query SELECT * FROM cat: %v", err)
		t.FailNow()
	}
	cur := conn.NewCursor()
	defer cur.Close()
	if err = cur.Execute("CREATE TABLE TST_table (key VARCHAR2(10), value VARCHAR2(100))", nil, nil); err != nil {
		t.Errorf("error creating TST_table: %v", err)
	}
	defer cur.Execute("DROP TABLE TST_table", nil, nil)

}
