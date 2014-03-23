#include "_cgo_export.h"

/*
void cCallback(dvoid *ctx, OCISubscription *subscrhp, dvoid *payload, ub4 paylen, dvoid *desc, ub4 mode) {
}

sword setSubsCallback(OCISubscription *subscrhp, OCIError *errhp) {
    return OCIAttrSet((dvoid *)subscrhp, OCI_HTYPE_SUBSCRIPTION,
                (OCISubscriptionNotify*)(&cCallback),
                0, OCI_ATTR_SUBSCR_CALLBACK,
				errhp);
}
*/


//OCISubscriptionNotify *callbackp = (OCISubscriptionNotify*)(&callback);
