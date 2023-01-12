package workflows

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-msvc/errors"
	"github.com/go-msvc/logger"
	"github.com/google/uuid"
)

type WorkflowFunc func(ctx Context, req any) (err error)

// SyncActionFunc is called once and completes its work in the foreground
// (synchronously) and it cannot receive continue events
// examples:
// - simple func that just calculated something, e.g. uuid.New().String())
// - blocking http call that waited for response on the same connection managed by this process)
// - simple call to time.Sleep() which did not use external scheduler for a delayed cont event)
type SyncActionFunc func(ctx Context, req any) (res any, err error)

// AsyncStartFunc is called once to begin something (e.g. send email)
// then context waits for an event that will call AsyncContFunc
// examples:
//   - sent a queue message, expecting one back
//   - scheduled a delayed continue event
//   - called an API that started another process that will tell us when it completed
//   - http call in a go routine on a connection managed by this process, with the
//     response event pushed into a channel then delivered to the workflow context
//     (sounds cool, but be careful if you stagger up a lot of pending requests, more then
//     the nr of workers you run... it is possible, but simpler to do blocking HTTP and
//     control the nr of workers you use to control the nr of pending http responses)
//
// IMPORTANT: the contID must be returned in the cont event along with the session id!
type AsyncActionStartFunc func(ctx Context, contID string, req any) (err error)
type AsyncActionContFunc func(ctx Context, res any) (result any, err error)

type Context interface {
	//context.Context
	ID() string
	OnceSync(action SyncActionFunc, data any) (result any, err error)
	OnceAsync(start AsyncActionStartFunc, cont AsyncActionContFunc, data any) (done bool, result any, err error)
}

type workflowContext struct {
	manager SessionManager
	//context.Context
	name              string //name of workflow
	id                string //uuid for this execution of the workflow
	yieldDataByCaller map[string]*yieldData
	run               *runContext  //current run
	runs              []runContext //all previous runs
}

func (ctx workflowContext) ID() string { return ctx.id }

func (ctx workflowContext) contCaller() string {
	nrRuns := len(ctx.runs)
	if nrRuns < 1 {
		return ""
	}
	lastRun := ctx.runs[len(ctx.runs)-1]
	if lastRun.yielded == nil {
		return ""
	}
	return lastRun.yielded.caller
} //ctx.contCaller()

func (ctx *workflowContext) OnceSync(action SyncActionFunc, data any) (result any, err error) {
	log.Debugf("Once() ...")
	_, result, err = ctx.once(action, nil, nil, data)
	return
} //workflowContext.Once()

func (ctx *workflowContext) OnceAsync(start AsyncActionStartFunc, cont AsyncActionContFunc, data any) (done bool, result any, err error) {
	log.Debugf("OnceAsync() ...")
	done, result, err = ctx.once(nil, start, cont, data)
	return
} //workflowContext.OnceAsync()

func (ctx *workflowContext) once(
	action SyncActionFunc,
	start AsyncActionStartFunc,
	cont AsyncActionContFunc,
	data any,
) (
	done bool,
	result any,
	err error,
) {
	if ctx.run.yielded != nil {
		//already yielded on a previous step, just skip over this step to end this run ASAP
		//it has not yet been called and will be called later once all previous steps completed
		return false, nil, nil
	}

	//get/define yield data for this step
	caller := fmt.Sprintf("%V", logger.GetCaller(3)) //todo: use stack of caller up to 3 levels back... to be sure it is unique for each call in the program and hash it may be for lookups? but store the full stack in code.
	yd, ok := ctx.yieldDataByCaller[caller]
	if !ok {
		yd = &yieldData{
			caller:  caller,
			started: false,
			done:    false,
			result:  nil,
			err:     nil,
			nrCalls: 0,
		}
		ctx.yieldDataByCaller[caller] = yd
		log.Debugf("New yield_data caller(%s): %+v", caller, yd)
	} else {
		log.Debugf("Existing yield_data caller(%s)", caller, yd)
	}

	yd.nrCalls++
	if !yd.started {
		log.Debugf("once(%s) start ...", caller)
		yd.started = true

		//worker is in a go-routine and can run concurrent to other workers
		//the activity called here should be quick, e.g. to send a queue message
		//and later expect a response, but to not wait for it...
		//
		//if it does a call in the foreground that takes time, it will block here
		//and keep the worker busy, which is required to prevent
		//to many go-routines being created if they happen to never complete or
		//all take long
		//
		//put once() around all calls that should be called once only, even if they
		//do not block or do not wait for continue, as yield will ensure they
		//are called once only and capture the result for subsequent runs and not
		//repeats the call (e.g. uuid may bot be called again as it will change the
		//behavior of the function)
		yd.actionStartTime = optionalTime(time.Now())
		if action != nil {
			//blocking action that completes without continue
			yd.result, yd.err = action(ctx, data)
			yd.done = true
		} else {
			//start that expects a continue
			yd.contID = uuid.New().String()
			yd.err = start(ctx, yd.contID, data)
		}
		yd.actionEndTime = optionalTime(time.Now())

		if yd.done {
			//sync action completed in foreground
			//return result,err now so workflow can proceed to next step
			log.Debugf("once(%s) sync action done in %v (not expecting continue event)", yd.caller, yd.startFuncDur())
			return true, yd.result, yd.err
		}

		//async action:
		//if failed, not expecting cont
		if yd.err != nil {
			yd.contID = "" //not expecting response if start failed!
			return true, nil, err
		}

		log.Debugf("once(%s) started in %v, waiting for continue event ...", yd.caller, yd.startFuncDur())
		ctx.run.yielded = yd //this tells us where this run yielded and prevents next steps until continued completes

		//register the cont action handler
		asyncContMutex.Lock()
		if _ /*f*/, ok := asyncContFuncByCaller[yd.caller]; ok {
			//todo: cannot compare func to each other in go - would be good to ensure we do not change the func after register!
			//it should work without the check, just want to be double sure
			// if f != cont {
			// 	return true, nil, errors.Errorf("cannot yield because contFunc[caller(%s)] already registered as %v() != this cont=%v() - internal error!", yd.caller, f, cont)
			// }
		} else {
			asyncContFuncByCaller[yd.caller] = cont
		}
		asyncContMutex.Unlock()

		//todo: schedule timeout event and cancel if cont is received
		return false, nil, nil
	} //if not yet started

	if !yd.done {
		//started and still waiting for continue event
		//should never get here as the workflow should only be called again when an event is received
		log.Errorf("once(%s) after %v and %d calls still waiting for response...", yd.caller, yd.dur(), yd.nrCalls)
		ctx.run.yielded = yd //this tells us where this run yielded and prevents next steps until continued completes
		return false, nil, nil
	}

	//done, return same result as before to proceed to next step
	log.Debugf("once(%s) done at %v in %v", yd.caller, yd.endTime(), yd.dur())
	return true, yd.result, yd.err
} //workflowContext.once()

// call Continue() to process continue event received with session id and correct contID
// discard events for this session with other contID values as they may be late or just bad
// this func return nil of continue was handled and workflow must be called again
func (ctx *workflowContext) Continue(contID string, data any) (err error) {
	caller := ctx.contCaller()
	log.Debugf("caller(%s): Continue(%s): data=(%T)%+v", caller, contID, data, data)
	yd, ok := ctx.yieldDataByCaller[caller]
	if !ok {
		return errors.Errorf("failed to get yield data for caller(%s) - internal error", caller)
	}
	if !yd.started || yd.done {
		return errors.Errorf("cannot cont with yd:%+v", yd)
	}

	//call async cont func determined by global data on caller
	//note: if workflow was recompiled, and code moved, this will fail!
	//todo: need to protect version of workflow and continue only in same version
	//or derive a clever way to find the same cont func in a new version that will
	//allow code change to fix blocked workflows after a restart! TODO...
	//IDEA: should be able to store the package and func name of caller may be as a more reliable way???
	//for now - just rely on caller
	asyncContMutex.Lock()
	cont, ok := asyncContFuncByCaller[caller]
	asyncContMutex.Unlock()
	if !ok {
		return errors.Errorf("cannot continue workflow - caller(%s) not found in this version", caller)
	}

	yd.contStartTime = optionalTime(time.Now())
	yd.result, yd.err = cont(ctx, data)
	yd.contEndTime = optionalTime(time.Now())
	yd.done = true
	yd.contID = ""
	//todo: unschedule timeout event

	//async action completed in foreground
	//return result,err now so workflow can proceed to next step or fail
	log.Debugf("once(%s) async action done start:%v wait:%v cont:%v total:%v", yd.caller, yd.startFuncDur(), yd.waitDur(), yd.contFuncDur(), yd.dur())
	return nil //retry workflow even if yd.err != nil - which will be returned to workflow from ctx.Once() so that workflow logic can deal with it
} //workflowContext.Continue()

// call Timeout() if not received cont event in expected time window
func (ctx *workflowContext) Timeout() {
	//get yield data for this step - it was defined when async action started
	//caller will not be in event and is retrieved from the current session data
	caller := ctx.contCaller()
	yd, ok := ctx.yieldDataByCaller[caller]
	if !ok {
		yd.err = errors.Errorf("failed to get yield data for caller(%s) - internal error", yd.caller)
		return
	}
	if !yd.started || yd.done {
		yd.err = errors.Errorf("cannot handle timeout with yd:%+v - internal error", yd)
		return
	}

	yd.result = nil                   //implied - written here to be clear on that
	yd.err = errors.Errorf("timeout") //todo: make easier for caller to see it was timeout and not asyncContFunc returning an error
	yd.contStartTime = optionalTime(time.Now())
	yd.contEndTime = optionalTime(time.Now())
	yd.done = true
	yd.contID = ""
	log.Debugf("once(%s) async timeout start:%v wait:%v cont:%v total:%v", yd.caller, yd.startFuncDur(), yd.waitDur(), yd.contFuncDur(), yd.dur())
	return //workflow will rerun and workflow func can see yd.err is timeout
} //workflowContext.Timeout()

func (ctx *workflowContext) Save() error {
	saved := SavedContext{
		ID:                ctx.id,
		Name:              ctx.name,
		YieldDataByCaller: map[string]SavedYieldData{},
		Runs:              []SavedRunContext{},
	}
	for caller, yd := range ctx.yieldDataByCaller {
		syd := SavedYieldData{
			Started: yd.started,
			NrCalls: yd.nrCalls,
			ContID:  yd.contID,
		}
		syd.ActionStartTime = yd.actionStartTime
		syd.ActionEndTime = yd.actionEndTime
		syd.ContStartTime = yd.contStartTime
		syd.ContEndTime = yd.contEndTime
		if yd.done {
			syd.Result = yd.result
			syd.Done = yd.done
			if yd.err != nil {
				syd.Err = fmt.Sprintf("%+v", yd.err)
			}
		}
		saved.YieldDataByCaller[caller] = syd
	}
	saved.Runs = make([]SavedRunContext, len(ctx.runs))
	for i, runContext := range ctx.runs {
		src := SavedRunContext{
			StartTime: runContext.startTime,
			EndTime:   runContext.endTime,
			AttemptNr: runContext.attemptNr,
		}
		if runContext.yielded != nil {
			src.YieldAt = runContext.yielded.caller
		} else {
			if runContext.err != nil {
				src.Err = fmt.Sprintf("%+v", runContext.err)
			}
		}
		saved.Runs[i] = src
	}
	if err := ctx.manager.Sync(saved); err != nil {
		return errors.Wrapf(err, "failed to sync session: %+v", err)
	}
	log.Debugf("Saved context: %+v", saved)
	return nil
} //workflowContext.Save()

func (ctx *workflowContext) Del() error {
	if err := ctx.manager.Del(ctx.id); err != nil {
		log.Errorf("failed to sync session: %+v", err)
	}
	log.Debugf("Deleted context")
	return nil
}

type runContext struct {
	workflowContext *workflowContext
	attemptNr       int //1,2,3,...
	startTime       time.Time
	endTime         time.Time
	yielded         *yieldData //set when yielded in this run
	err             error
}

type yieldData struct {
	caller          string
	started         bool
	actionStartTime *time.Time //time when action func started
	actionEndTime   *time.Time //time when action func ended
	contStartTime   *time.Time //time when cont func started
	contEndTime     *time.Time //time when cont func ended
	contID          string     //id expected in continue event for this session to ensure a late cont event that workflow already timed out, does not trigger the next cont
	done            bool
	result          interface{}
	err             error
	nrCalls         int
}

func (yd yieldData) startFuncDur() time.Duration {
	if yd.actionStartTime == nil || yd.actionEndTime == nil {
		return 0
	}
	return (*yd.actionEndTime).Sub(*yd.actionStartTime)
}

func (yd yieldData) contFuncDur() time.Duration {
	if yd.contStartTime == nil || yd.contEndTime == nil {
		return 0
	}
	return (*yd.contEndTime).Sub(*yd.contStartTime)
}

func (yd yieldData) waitDur() time.Duration {
	if yd.actionEndTime == nil || yd.contStartTime == nil {
		return 0
	}
	return (*yd.contStartTime).Sub(*yd.actionEndTime) //time since start completed and began processing the cont event
}

func (yd yieldData) dur() time.Duration {
	if yd.contStartTime == nil {
		return 0 //not yet started
	}
	if yd.contEndTime == nil {
		return time.Now().Sub(*yd.actionStartTime) //i.e. total time from start until now still waiting for/processing cont
	}
	//done, received and processed cont
	return (*yd.contEndTime).Sub(*yd.actionStartTime) //i.e. total time including wait for cont time
}

func (yd yieldData) endTime() time.Time {
	if yd.contEndTime != nil {
		return *yd.contEndTime
	}
	if yd.actionEndTime != nil {
		return *yd.actionEndTime
	}
	return time.Now() //should not get here
}

// struct used to save session data as JSON document
type SavedContext struct {
	ID                string                    `json:"id"`
	Name              string                    `json:"name" doc:"Name of workflow"`
	YieldDataByCaller map[string]SavedYieldData `json:"yield_data_by_caller"`
	Runs              []SavedRunContext         `json:"runs"`
}

// convert from save to context that can be used internally
func (saved SavedContext) Context() *workflowContext {
	//convert to context data
	ctx := &workflowContext{
		id:                saved.ID,
		name:              saved.Name,
		yieldDataByCaller: map[string]*yieldData{},
		runs:              []runContext{},
	}
	for caller, syd := range saved.YieldDataByCaller {
		yd := yieldData{
			caller:  caller,
			started: syd.Started,
			done:    syd.Done,
			nrCalls: syd.NrCalls,
		}
		yd.actionStartTime = syd.ActionStartTime
		yd.actionEndTime = syd.ActionEndTime
		yd.contStartTime = syd.ContStartTime
		yd.contEndTime = syd.ContEndTime
		if yd.done {
			yd.result = syd.Result
			if syd.Err != "" {
				yd.err = errors.Error(syd.Err)
			}
		}
		ctx.yieldDataByCaller[caller] = &yd
	}
	ctx.runs = make([]runContext, len(saved.Runs))
	for i, sr := range saved.Runs {
		rc := runContext{
			workflowContext: ctx,
			attemptNr:       sr.AttemptNr,
			startTime:       sr.StartTime,
			endTime:         sr.EndTime,
		}
		if sr.Err != "" {
			rc.err = errors.Error(sr.Err)
		}
		if sr.YieldAt != "" {
			rc.yielded = ctx.yieldDataByCaller[sr.YieldAt]
		}
		ctx.runs[i] = rc
	}
	return ctx
}

type SavedYieldData struct {
	Started         bool        `json:"started"`
	Done            bool        `json:"done"`
	ActionStartTime *time.Time  `json:"action_start_time,omitempty"`
	ActionEndTime   *time.Time  `json:"action_end_time,omitempty"`
	ContStartTime   *time.Time  `json:"cont_start_time,omitempty"`
	ContEndTime     *time.Time  `json:"cont_end_time,omitempty"`
	ContID          string      `json:"cont_id,omitempty"`
	Result          interface{} `json:"result,omitempty"`
	Err             string      `json:"error,omitempty"`
	NrCalls         int         `json:"nr_calls" doc:"Number of times this func was called to complete"`
}

type SavedRunContext struct {
	AttemptNr int       `json:"attempt_nr"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	YieldAt   string    `json:"yield_at,omitempty" doc:"one of yield_at, error or completed will be defined"`
	Err       string    `json:"error,omitempty" doc:"one of yield_at, error or completed will be defined"`
	Completed bool      `json:"completed,omitempty" doc:"one of yield_at, error or completed will be defined"`
}

func optionalTime(t time.Time) *time.Time { return &t }

type timeOutEvent struct{}

var (
	asyncContMutex        sync.Mutex
	asyncContFuncByCaller = map[string]AsyncActionContFunc{}
	started               bool
	//started: after started - no new entries will be allowed - else test cases did not cover all code paths
	//todo: provide a way that use can register and run all test cases and list registered functions
	//and search code for them all to ensure they all were registered!
)

//todo: prerun all workflows with a list of defined []prerun values registered as part of the workflow
//by which they must pass through all their OnceWithCont() calls to register them but we wont call the start func
//so that all caller cont functions can be registered before we start a workflow, as we may have to
//process continue events from existing sessions before we started and registered all cont funcs otherwise...!
//see if that is viable without complicating the action implementations
//this can serve well as a list of test cases needed per workflow - with some documentation!
