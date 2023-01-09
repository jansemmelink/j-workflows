package main

import (
	"fmt"
	"time"

	"github.com/go-msvc/errors"
	"github.com/stewelarend/logger"
)

var log = logger.New().WithLevel(logger.LevelDebug)

//see if can hide yields from user

type Instance interface {
	YieldData(caller string) *yieldData
}

type instance struct {
	yieldByCaller map[string]*yieldData

	runs []*instanceRun
}

func newInstance() *instance {
	return &instance{
		yieldByCaller: map[string]*yieldData{},
		runs:          []*instanceRun{},
	}
}

func (inst instance) YieldData(caller string) *yieldData {
	if yd, ok := inst.yieldByCaller[caller]; ok {
		return yd
	}
	yd := &yieldData{
		caller:  caller,
		started: false,
		done:    false,
		result:  nil,
		err:     nil,
	}
	inst.yieldByCaller[caller] = yd
	return yd
}

func (inst *instance) Run(fnc func(ctx Instance) error) (*yieldData, error) {
	thisRun := &instanceRun{}
	inst.runs = append(inst.runs, thisRun)

	err := fnc(inst)
	if thisRun.yielded != nil {
		return thisRun.yielded, nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "failed and not yielded")
	}
	return nil, nil //completed successfully
}

type instanceRun struct {
	start   time.Time
	end     time.Time
	yielded *yieldData
}

func main() {
	inst := newInstance()
	for i := 0; i < 10; i++ {
		log.Debugf("RUN %d", i+1)
		yd, err := inst.Run(doSum)
		if err != nil {
			panic(fmt.Sprintf("run failed: %+v", err))
		}
		if yd == nil {
			break
		}
		log.Debugf("Yielded at %s ...", yd.caller)
		time.Sleep(400 * time.Millisecond)
	}
	log.Debugf("Done")
}

func doSum(ctx Instance) error {
	a := pickNr(ctx)
	b := pickNr(ctx)
	c := a * b
	send(ctx, c)
	return nil
}

// make it a workflow func for async execution
func pickNr(ctx Instance) int {
	v, err := yield(ctx, slowPickNr)
	log.Debugf("pickNr() -> %v,%v", v, err)
	if err != nil {
		return 0
	}
	return v.(int)
}

// a slow function
var nrs = []int{5, 9, 11, 3}

func slowPickNr(ctx Instance) (interface{}, error) {
	time.Sleep(time.Second)
	if len(nrs) == 0 {
		return 0, errors.Errorf("no more numbers")
	}
	nr := nrs[0]
	nrs = nrs[1:]
	return nr, nil
}

//todo: external session (see if can use S3 and SQS)
//todo: things that start and continue, e.g. send http
//todo: or fire and wait for event like send ms request to nats and process response from queue
//todo: consume rather than poll, but still support poll as well for things that cannot send continue
//todo: limits

func send(ctx Instance, v int) {
	fmt.Printf("sending %+v\n", v)
}

type actionFunc func(ctx Instance) (interface{}, error)

type yieldData struct {
	caller  string
	started bool
	start   time.Time
	end     time.Time
	done    bool
	result  interface{}
	err     error
}

type CtxYieldInfoKey logger.Caller

func yield(ctx Instance, fnc actionFunc) (interface{}, error) {
	//get source code reference to line in workflow that called this function
	caller := fmt.Sprintf("%+v", logger.GetCaller(3)) //e.g. "main.go(45)"

	runs := ctx.(*instance).runs
	thisRun := runs[len(runs)-1]
	if thisRun.yielded != nil {
		log.Debugf("yielded, skip %s", caller)
		return nil, errors.Errorf("yielded") //already yielded on other step, fail this step
	}

	//not yet yielded in this run
	//check this caller
	yd := ctx.YieldData(caller)
	if !yd.started {
		log.Debugf("yield(%s) starting...", caller)
		thisRun.yielded = yd
		yd.started = true
		yd.start = time.Now()
		go func() {
			value, err := fnc(ctx)
			log.Debugf("yield(%s) done -> (%T)%v, %v", yd.caller, value, value, err)
			yd.result = value
			yd.err = err
			yd.done = true
			yd.end = time.Now()
		}()
		return nil, errors.Errorf("yield(%s) started ... need to call again", caller)
	}

	if !yd.done {
		thisRun.yielded = yd
		return nil, errors.Errorf("yield(%s) busy ... need to call again", caller)
	}
	return yd.result, yd.err
} //yield()
