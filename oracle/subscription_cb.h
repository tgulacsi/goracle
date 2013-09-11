#include <oci.h>

//ub4 callback(dvoid *ctx , OCISubscription *subscrhp, dvoid *payload, ub4 paylen, dvoid *desc, ub4 mode);
//ub4 callback(dvoid *ctx , dvoid *subscrhp, dvoid *payload, ub4 paylen, dvoid *desc, ub4 mode);
extern sword setSubsCallback(OCISubscription *subscrhp, OCIError *errhp, OCISubscriptionNotify *cb);
extern ub4 callback(dvoid *ctx, OCISubscription *subscrhp, dvoid *payload, ub4 paylen, dvoid *desc, ub4 mode);

extern OCISubscriptionNotify *callbackp;
