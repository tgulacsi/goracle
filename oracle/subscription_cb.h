#include <oci.h>

//extern sword setSubsCallback(OCISubscription *subscrhp, OCIError *errhp);
//extern ub4 callback(dvoid *ctx, OCISubscription *subscrhp, dvoid *payload, ub4 paylen, dvoid *desc, ub4 mode);
extern ub4 goCallback(dvoid *ctx, OCISubscription *subscrhp, dvoid *payload, ub4 paylen, dvoid *desc, ub4 mode);

//extern OCISubscriptionNotify *callbackp;
