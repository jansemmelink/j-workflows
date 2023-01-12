package workflows

import (
	"fmt"
	"sync"
	"time"

	"github.com/stewelarend/logger"
)

var log = logger.New().WithLevel(logger.LevelDebug)

type Workflows interface {
	With(name string, fnc WorkflowFunc) Workflows
	Run(sm SessionManager, es EventStream, nrWorkers int) error
}

func New() Workflows {
	return workflows{
		workflow: map[string]WorkflowFunc{},
	}
}

type workflows struct {
	workflow map[string]WorkflowFunc
}

func (w workflows) With(name string, fnc WorkflowFunc) Workflows {
	w.workflow[name] = fnc
	return w
}

func (w workflows) Run(sm SessionManager, es EventStream, nrWorkers int) error {
	//start concurrent workers
	//they will run until workerChan is closed
	workerChan := make(chan Event)
	wg := sync.WaitGroup{}
	for i := 0; i < nrWorkers; i++ {
		wg.Add(1)
		go func(workerIndex int) {
			w.worker(workerIndex, sm, workerChan, es)
			wg.Done()
		}(i)
	}
	log.Debugf("Started %d workers", nrWorkers)

	//start consuming the eventStream
	//close the event stream to stop running
	for event := range es.Chan() {
		log.Debugf("Processing event %+v", event)
		if err := event.Validate(); err != nil {
			log.Errorf("discard invalid event: %+v", err)
			continue
		}
		if event.IsStart() || event.IsCont() {
			//push to worker chan - which we use to limit the nr of concurrent processing we take on
			//if all workers are busy, this will block until one becomes available
			//and all workers should cycle very quickly
			workerChan <- event
			continue
		}
		log.Errorf("discard event which is not start nor continue: %+v", event)
		continue
	} //for events from queue

	log.Debugf("Stopped consuming event stream")
	log.Debugf("Stopping workers")
	close(workerChan)
	wg.Wait()

	log.Debugf("All workers stopped")
	return nil
} //workflows.Run()

func (w workflows) worker(workerIndex int, sm SessionManager, eventChan <-chan Event, es EventStream) {
	log := log.New(fmt.Sprintf("worker[%d]", workerIndex)).WithLevel(logger.LevelDebug)
	log.Debugf("Worker Started")
	//todo: see why logger does not print its name... maybe use logger.With() rather

	var workflowFunc WorkflowFunc
	var ctx Context
	for event := range eventChan {
		if event.IsStart() {
			log.Debugf("processing start %+v event from %s at %v data:%+v", *event.Start, event.Source, event.Time, event.Data)
			//process this start event in a new context
			var ok bool
			workflowFunc, ok = w.workflow[event.Start.WorkflowName]
			if !ok {
				log.Errorf("unknown workflow(%s)", event.Start.WorkflowName)
				continue //todo: reshedule
			}

			var err error
			ctx, err = sm.New(event.Start.WorkflowName)
			if err != nil {
				log.Errorf("failed to create context: %+v", err)
				continue //todo: reschedule?
			}
		} else {
			//continue event
			log.Debugf("processing cont %+v event source(%s) at %v data:%+v", *event.Cont, event.Source, event.Time, event.Data)

			var err error
			ctx, err = sm.Get(event.Cont.SessionID)
			if err != nil {
				log.Errorf("Discard cont event for %s: %+v", event.Cont.SessionID, err)
				continue //todo: reschedule?
			}
			//todo: measure context retrieval and update durations! Takes quite long with S3...
			var ok bool
			_ctx := ctx.(*workflowContext)
			log.Debugf("retrieved _ctx: %+v", ctx)

			//handle the event, which will call the OnceWithCont()'s AsyncActionContFunc...
			if err := _ctx.Continue(event.Cont.ContID, event.Data); err != nil {
				log.Errorf("Failed to process cont %+v event source(%s) data:%+v: %+v", *event.Cont, event.Source, event.Data, err)
				continue //would not help to call workflow again - nothing changed, event might have been invalid of old event, badly addressed etc
			}

			//continue event processed for this context, so now need to re-run the workflow to complete the yielding action
			workflowFunc, ok = w.workflow[_ctx.name]
			if !ok {
				log.Errorf("unknown workflow(%s)", _ctx.name)
				continue //todo: reshedule?
			}

			//start a new run in this context
			lastRun := _ctx.runs[len(_ctx.runs)-1]
			_ctx.run = &runContext{
				workflowContext: _ctx,
				attemptNr:       lastRun.attemptNr + 1,
				startTime:       time.Now(),
			}
		} //if cont

		//type assert public interface to internal struct so we can access its members
		_ctx := ctx.(*workflowContext)
		log.Debugf("execute in _ctx: %+v", ctx)
		log.Debugf("run: %+v", *_ctx.run)

		//execute the workflow func
		_ctx.run.err = workflowFunc(ctx, nil)
		_ctx.run.endTime = time.Now()
		log.Debugf("run[%d] took %v", _ctx.run.attemptNr, _ctx.run.endTime.Sub(_ctx.run.startTime))

		_ctx.runs = append(_ctx.runs, *_ctx.run)

		if _ctx.run.yielded != nil {
			log.Debugf("yielded at %v", _ctx.run.yielded.caller)
			_ctx.run = nil
			if err := _ctx.Save(); err != nil {
				log.Errorf("failed to save context: %+v", err)
			}
			//todo: schedule timeout
		} else {
			if _ctx.run.err != nil {
				log.Errorf("failed: %+v", _ctx.run.err)
			} else {
				log.Debugf("success")
			}
			if err := _ctx.Del(); err != nil {
				log.Errorf("failed to delete context: %+v", err)
			}
		}
	} //for event chan
	log.Debugf("Worker Stopped")
} //workflows.worker()
