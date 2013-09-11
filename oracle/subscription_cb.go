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
#include <stdlib.h>
#include <oci.h>
*/
import "C"

import (
	"log"
	"unsafe"
)

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
