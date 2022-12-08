package main

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/jansemmelink/j-workflow/workflows"
	"github.com/stewelarend/logger"
)

var log = logger.New().WithLevel(logger.LevelDebug)

func main() {
	//start engine in background
	engine, err := workflows.New().
		With("sum", doSum).
		Start()
	if err != nil {
		panic(fmt.Sprintf("failed to start: %+v", err))
	}

	//start N calls a few milliseconds apart
	n := 10
	for i := 0; i < n; i++ {
		if /*id*/ _, err := engine.Start("sum", nil); err != nil {
			panic(fmt.Sprintf("failed to start: %+v", err))
			// } else {
			// 	log.Debugf("Started id=%s", id)
		}
		//time.Sleep(time.Millisecond * 300)
	}

	//log.Debugf("Waiting...")
	<-engine.Stop()
	//log.Debugf("Terminated.")
} //main()

//todo: start sub workflows and wait for them to complete
//todo: do actions/sub-workflow/a mix in parallel and wait
//todo: deal with handlers that return an error
//todo: deal with handlers that return custom types
//todo: limit exec time
//todo: log the full flow
//todo: scale over many instances with shared persistent data, consider multiple workflow implementations - independant? - and multiple versions to also run independent, and how to know when old one is no longer in use
//todo: scheduled tasks - e.g. shedule to continue at a specific time - combine with generic scheduler

type sumAudit struct {
	Args  []int  `json:"args"`
	Oper  string `json:"oper"`
	Total int    `json:"total"`
}

func doSum(ctx context.Context /*, req MyRequestType*/) sumAudit {
	rt := ctx.Value(workflows.CtxRuntime{}).(workflows.Runtime)
	audit := sumAudit{}
	a := rt.YieldInt(pickNumber())
	b := rt.YieldInt(pickNumber())
	audit.Args = []int{a, b}
	if a < b {
		audit.Oper = "+"
		audit.Total = rt.YieldInt(sum(a, b))
	} else {
		audit.Oper = "-"
		audit.Total = rt.YieldInt(sub(a, b))
	}
	return audit
}

func pickNumber() workflows.Action {
	return func() (interface{}, error) {
		v := rand.Intn(100)
		return v, nil
	}
}

func sum(a int, b int) workflows.Action {
	return func() (interface{}, error) {
		return a + b, nil
	}
}

func sub(a int, b int) workflows.Action {
	return func() (interface{}, error) {
		return a - b, nil
	}
}
