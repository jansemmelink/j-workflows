package workflows

import (
	"context"
	"fmt"
	"reflect"
	"regexp"

	"github.com/stewelarend/logger"
)

var log = logger.New() //.WithLevel(logger.LevelDebug)

type Workflows interface {
	With(name string, hdlr interface{}) Workflows
	Start() (Engine, error)
}

func New() Workflows {
	wf := workflows{
		workflowByName: map[string]workflow{},
	}
	return wf
} //New()

type workflows struct {
	workflowByName map[string]workflow
}

var contextInterface = reflect.TypeOf((*context.Context)(nil)).Elem()

const namePattern = `[a-zA-Z]([a-zA-Z0-9_-]*[a-zA-Z0-9])*`

var nameRegex = regexp.MustCompile("^" + namePattern + "$")

func (wf workflows) With(name string, hdlr interface{}) Workflows {
	if !nameRegex.MatchString(name) {
		panic(fmt.Sprintf("invalid name(%s)", name))
	}
	workflow := workflow{
		name:      name,
		hdlrValue: reflect.ValueOf(hdlr),
	}

	//check hdlr has correct func prototype
	hdlrType := reflect.TypeOf(hdlr)
	if hdlrType.Kind() != reflect.Func {
		panic(fmt.Sprintf("%s.hdlr is not a func", name))
	}
	if hdlrType.NumIn() < 1 || !hdlrType.In(0).Implements(contextInterface) {
		panic(fmt.Sprintf("%s.hdlr does not take context.Context as first argument", name))
	}
	if hdlrType.NumIn() > 2 {
		panic(fmt.Sprintf("%s.hdlr take more arguments than (context.Context,reqType)", name))
	}
	if hdlrType.NumIn() > 1 {
		workflow.reqType = hdlrType.In(1)
	}
	if hdlrType.NumOut() > 1 {
		panic(fmt.Sprintf("%s.hdlr returns more results than just audit record", name))
	}
	if hdlrType.NumOut() == 1 {
		workflow.auditType = hdlrType.Out(0)
	}
	wf.workflowByName[name] = workflow
	return wf
} //workflows.With()

func (wf workflows) Start() (Engine, error) {
	return startEngine(wf)
}
