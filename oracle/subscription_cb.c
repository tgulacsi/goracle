#include "_cgo_export.h"

sword setSubsCallback(OCISubscription *subscrhp, OCIError *errhp, OCISubscriptionNotify *cb) {
//sword setSubsCallback(dvoid *subscrhp, OCIError *errhp) {
    return OCIAttrSet((dvoid *)subscrhp, OCI_HTYPE_SUBSCRIPTION,
                cb,
				//&callback,
                0, OCI_ATTR_SUBSCR_CALLBACK,
				errhp);
}

OCISubscriptionNotify *callbackp = (OCISubscriptionNotify*)(&callback);
