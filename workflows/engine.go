package workflows

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/go-msvc/errors"
)

type Engine interface {
	//starts background processing of the named workflow using the speficied request data
	//returns new instance id if started, else error
	Start(name string, req interface{}) (string, error)
	Stop() <-chan bool //unsubscribe, no new starts allowed, wait until all current tasks completed
}

type engine struct {
	wf                  workflows
	mutex               sync.Mutex
	instanceByID        map[string]*instance
	readyInstanceIDChan chan string
	stoppedChan         chan bool
	auditor             Auditor
}

func (e *engine) Start(workflowName string, req interface{}) (string, error) {
	if e.stoppedChan != nil {
		return "", errors.Errorf("stopping: will not start more workflows")
	}
	//get definition of named workflow
	workflow, ok := e.wf.workflowByName[workflowName]
	if !ok {
		return "", errors.Errorf("unknown workflow name(%s)", workflowName)
	}

	//validate the request for this workflow
	if workflow.reqType != nil {
		if workflow.reqType != reflect.TypeOf(req) {
			//need type conversion: marshal to JSON then unmarshal into type
			jsonReq, err := json.Marshal(req)
			if err != nil {
				return "", errors.Wrapf(err, "failed to marshal %T to JSON", req)
			}
			reqPtrValue := reflect.New(workflow.reqType)
			if err := json.Unmarshal(jsonReq, reqPtrValue.Interface()); err != nil {
				return "", errors.Wrapf(err, "failed to unmarshal JSON req into %v", workflow.reqType)
			}
			if validator, ok := reqPtrValue.Interface().(Validator); ok {
				if err := validator.Validate(); err != nil {
					return "", errors.Wrapf(err, "invalid request")
				}
				log.Debugf("Validated %v", workflow.reqType)
			} else {
				log.Debugf("Not validating %v", workflow.reqType)
			}
			req = reqPtrValue.Elem().Interface()
		} else {
			//passed correct type, do validation before start
			if validator, ok := req.(Validator); ok {
				if err := validator.Validate(); err != nil {
					return "", errors.Wrapf(err, "invalid request")
				}
				log.Debugf("Validated %T", req)
			} else {
				log.Debugf("Not validating %T", req)
			}
		}
	} else {
		req = nil
	}

	log.Debugf("Request: (%T)%+v", req, req)

	inst := newInstance(workflow, req) //must persists when multiple processes
	e.instanceByID[inst.id] = inst
	e.readyInstanceIDChan <- inst.id
	return inst.id, nil
} //engine.Start()

func startEngine(wf workflows) (Engine, error) {
	e := &engine{
		wf:                  wf,
		instanceByID:        map[string]*instance{},
		readyInstanceIDChan: make(chan string),
		stoppedChan:         nil,
		auditor:             fileAuditor{file: os.Stdout, encoder: jsonAuditEncoder{}},
	}
	//start event consumer
	go e.consumer()

	return e, nil
} //startEngine()

type CtxRuntime struct{}

func (e *engine) consumer() {
	log.Debugf("STARTED processing session ids ...")
	for id := range e.readyInstanceIDChan {
		//check stopped - so we can wake an idle workflows engine to stop by calling Stop() that push empty id
		if id == "STOP" {
			if e.stoppedChan != nil && len(e.instanceByID) == 0 {
				break
			}
			continue
		}

		e.mutex.Lock()
		inst, ok := e.instanceByID[id]
		e.mutex.Unlock()
		if !ok {
			log.Errorf("inst(%s) not found", id)
			continue
		}

		//create a new runtime for this instance
		rt := &runtime{
			engine:    e,
			inst:      inst,
			callIndex: 0,
			yielded:   false,
		}
		ctx := context.Background()
		ctx = context.WithValue(ctx, CtxRuntime{}, rt)
		log.Debugf("inst(%s) start exec", inst.id)

		//call the workflow handler with the context and request
		//this should be non-blocking function that executes extremely quickly

		//todo: do in background with time limit to force workflows to fail
		//if they do not run quickly

		hdlrArgs := []reflect.Value{reflect.ValueOf(ctx)}
		if inst.workflow.reqType != nil {
			hdlrArgs = append(hdlrArgs, reflect.ValueOf(rt.inst.req))
		}
		hdlrResults := inst.workflow.hdlrValue.Call(hdlrArgs)
		if rt.yielded {
			log.Debugf("inst(%s) exec yielded", inst.id)
		} else {
			e.mutex.Lock()
			delete(e.instanceByID, inst.id)
			log.Debugf("inst(%s) DONE, %d remain", inst.id, len(e.instanceByID))
			e.mutex.Unlock()

			if e.auditor != nil {
				var auditData interface{}
				if len(hdlrResults) == 1 {
					auditData = hdlrResults[0].Interface()
					log.Debugf("audit: (%T)%+v", auditData, auditData)
				}
				tEnd := time.Now()
				e.auditor.Write(
					rt.inst.startTime, //time when workflow started first time
					tEnd,              //time when completed
					nil,               //error - not failed
					//exec.inst.execCount, //nr of time executed (=nr times yielded + 1)
					//average exec time
					//max exec time
					auditData,
				)
			}
		}
		if e.stoppedChan != nil && len(e.instanceByID) == 0 {
			break
		}
		log.Debugf("Wait for more events on %d instances", len(e.instanceByID))
	} //for ready sessions

	log.Debugf("STOPPED processing")
	e.stoppedChan <- true
} //engine.consumer()

func (e *engine) Stop() <-chan bool {
	if e.stoppedChan != nil {
		panic("workflows engine.Stop() called multiple times")
	}
	e.stoppedChan = make(chan bool)

	log.Debugf("Stopping...")

	//poke the engine to stop in case its already idle
	go func() {
		e.readyInstanceIDChan <- "STOP"
	}()

	return e.stoppedChan
}

type Runtime interface {
	Yield(Action) interface{}
	YieldInt(Action) int
	YieldString() string
}

type runtime struct {
	engine       *engine   //reference to the engine that created this runtime
	inst         *instance //reference to the workflow instance
	callIndex    int
	yielded      bool
	failedAction *action
}

type hdlrResult struct {
	value interface{}
	err   error
}

func (exec *runtime) Yield(f Action) interface{} {
	ci := exec.callIndex
	exec.callIndex++

	var thisAction *action
	if ci >= len(exec.inst.actions) { //first time session execute to this point
		thisAction = &action{
			running:    false,
			resultChan: make(chan hdlrResult),
			completed:  false,
			value:      nil,
		}
		go func() {
			result := <-thisAction.resultChan
			close(thisAction.resultChan)
			thisAction.resultChan = nil
			thisAction.running = false

			if result.err != nil {
				thisAction.err = result.err
				exec.failedAction = thisAction
			} else {
				thisAction.value = result.value
				thisAction.completed = true
				log.Debugf("session(%s): Got value: (%T)%+v", exec.inst.id, thisAction.value, thisAction.value)
			}

			//push session id to channel to notify it is ready to call again
			exec.engine.readyInstanceIDChan <- exec.inst.id
		}()
		exec.inst.actions = append(exec.inst.actions, thisAction)
	} else {
		thisAction = exec.inst.actions[ci]
	}

	if thisAction.completed {
		log.Debugf("session(%s): DONE [%d]: (%T)%+v", exec.inst.id, ci, thisAction.value, thisAction.value)
		return thisAction.value //return known value so can proceed to next step
	}

	if exec.yielded {
		log.Debugf("session(%s): SKIP [%d]: %+v", exec.inst.id, ci, *thisAction)
		return nil //yielded for a previous call - skip over to end
	}

	exec.yielded = true //yield because either this call is running or starting to run
	if thisAction.running {
		//this call is running, yield while waiting for response
		log.Debugf("session(%s): WAIT [%d]: %+v", exec.inst.id, ci, *thisAction)
		return nil
	}

	//start call of func in background() to get this value
	log.Debugf("session(%s): CALL [%d] ...", exec.inst.id, ci)
	thisAction.running = true
	go func() {
		value, err := f() //arguments are from caller that wrapped it in a here-func
		thisAction.resultChan <- hdlrResult{value, err}
	}()
	return nil
} //yield

func (exec *runtime) YieldInt(f Action) int {
	v, ok := exec.Yield(f).(int)
	if !ok {
		return 0
	}
	return v
}

func (exec *runtime) YieldString(f Action) string {
	v, ok := exec.Yield(f).(string)
	if !ok {
		return ""
	}
	return v
}

type Validator interface {
	Validate() error
}
