package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-msvc/errors"
	aws "github.com/jansemmelink/j-workflow/aws"
	workflows "github.com/jansemmelink/j-workflow/workflows2"
	"github.com/stewelarend/logger"
)

var log = logger.New().WithLevel(logger.LevelDebug)

func main() {
	qname := flag.String("qname", "Workflow-Test", "qname to consume in SQS")
	flag.Parse()

	myAWS, err := aws.New()
	if err != nil {
		panic(fmt.Sprintf("failed to connect with AWS: %+v", err))
	}

	s3, err := myAWS.S3()
	if err != nil {
		panic(fmt.Sprintf("failed to connect to S3: %+v", err))
	}
	sm, err := workflows.NewSessions(s3, "workflow-sessions-"+strings.ToLower(*qname))
	if err != nil {
		panic(fmt.Sprintf("failed to create S3 session manager: %+v", err))
	}

	es, err := myAWS.SQS(*qname, true)
	if err != nil {
		panic(fmt.Sprintf("failed to create SQS event stream: %+v", err))
	}

	//start mock service res processor to push mock service responses into event stream to continue workflows
	resChan = make(chan map[string]interface{})
	go func() {
		for res := range resChan {
			log.Debugf("Received res (%T)%+v", res, res)
			contEvent := workflows.Event{
				Cont: &workflows.ContEvent{
					SessionID: res["session_id"].(string),
					ContID:    res["cont_id"].(string),
				},
				Source: "internal mock service",
				Time:   time.Now(),
				Data:   res,
			}
			if err := es.PushContinue(contEvent); err != nil {
				log.Errorf("Failed to push continue: %+v", err)
			} else {
				log.Debugf("Pushed cont:%+v source(%s) data:(%T)%+v", *contEvent.Cont, contEvent.Source, contEvent.Data, contEvent.Data)
			}
		}
	}()

	if err := workflows.New().
		With("sum", sumWorkflow).
		Run(sm, es, 2); err != nil {
		panic(fmt.Sprintf("failed to run workflows: %+v", err))
	}
}

// this is the workflow that the user will write, hiding the yields
func sumWorkflow(ctx workflows.Context, req interface{}) error {
	a := pickNr(ctx)
	b := pickNr(ctx)
	c := a * b
	send(ctx, c)
	return nil
}

//todo: child workflows, concurrencies and iterators on calls and child workflows and slow operations
//todo: work with other and custom types - all able to marshal/unmarshal (or else serialize to []byte)

// actions that can be pre-built and custom
// make it a workflow func for async execution
func pickNr(ctx workflows.Context) int {
	v, err := ctx.OnceSync(actionPickNr, nil)
	log.Debugf("pickNr() -> %v,%v", v, err)

	//slowPickNr never fails... but if it could, handle it
	// if err != nil {
	// 	return 0
	// }

	//cached value is parsed from JSON and could be float64
	//so support conversion to int
	//todo: see if response type can be passed or derived inside Once() by looking at result of func...
	//this this will not be required, and also support other types
	//could also then discard the logic in this func and just return v regardless as received from call to Once()
	if i64, err := strconv.ParseInt(fmt.Sprintf("%v", v), 10, 64); err != nil {
		log.Errorf("failed to parse (%T)%v as int64", v, v)
		return 0
	} else {
		return int(i64)
	}
} //pickNr

// a blocking action function (without continue event)
var nrs = []int{5, 9, 11, 3}

func actionPickNr(ctx workflows.Context, notUsedReq interface{}) (interface{}, error) {
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

// simulate an external service by using a channel where we can send request
// and receive response
type service struct {
	reqChan chan serviceRequest
}

type serviceRequest struct {
	sessionID string
	contID    string
	data      interface{}
	resChan   chan map[string]interface{}
}

func (s *service) Run() {
	s.reqChan = make(chan serviceRequest)
	go func() {
		for req := range s.reqChan {
			log.Debugf("Processing service req (%T)%+v ...", req, req)

			//service processing ... simulate time it takes
			time.Sleep(time.Second)

			//send response
			res := map[string]interface{}{
				//header
				"session_id": req.sessionID,
				"cont_id":    req.contID,
				//service response data (hard coded for now)
				"response": 123,
			}
			req.resChan <- res
		}
	}()
}

var s service

var resChan chan map[string]interface{}

func init() {
	s.Run()
}

// send something out on a queue, expecting a response or timeout
// not keeping a go-routine running for this, e.g. when communicate
// via nats or SQS etc...
func send(ctx workflows.Context, v int) (interface{}, error) {
	done, res, err := ctx.OnceAsync(startSend, contRecv, v)
	log.Debugf("send() -> %v,%v", done, err)
	if !done {
		return nil, errors.Errorf("yielded")
	}
	if err != nil {
		return nil, errors.Wrapf(err, "failed to send")
	}
	return res, nil
}

func startSend(ctx workflows.Context, contID string, req any) (err error) {
	//send if not yet sent/yielded for another step
	fmt.Printf("sending request (%T)%+v\n", req, req)
	s.reqChan <- serviceRequest{
		sessionID: ctx.ID(),
		contID:    contID,
		data:      req,
		resChan:   resChan,
	}
	//return false to indicate need to wait until response is received (todo: or timeout)
	return nil
}

// send request and wait for response in a go-routing, e.g. for HTTP
// where response is quick and on the same connection
func contRecv(ctx workflows.Context, res any) (result any, err error) {
	log.Errorf("NYI(res=(%T)%+v")
	//now need to yield until response is received or timeout
	return nil, errors.Errorf("NYI")
}
