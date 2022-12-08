package workflows

import "reflect"

type workflow struct {
	name      string
	reqType   reflect.Type
	auditType reflect.Type
	hdlrValue reflect.Value //func(context.Context, myRequestType)
}
