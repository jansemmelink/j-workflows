package workflows

import (
	"time"

	"github.com/google/uuid"
)

type Instance interface {
	ID() string
	Name() string
	Request() interface{}
	StartTime() time.Time
}

type instance struct {
	id        string
	workflow  workflow
	req       interface{}
	startTime time.Time
	execCount int
	actions   []*action
}

func newInstance(workflow workflow, req interface{}) *instance {
	return &instance{
		id:        uuid.New().String(),
		workflow:  workflow,
		req:       req,
		startTime: time.Now(),
		execCount: 0,
		actions:   []*action{},
	}
}

type action struct {
	running    bool
	resultChan chan hdlrResult
	completed  bool
	//when completed, either value or err is set, (both could be nil, but err != nil when failed)
	value interface{}
	err   error
}

func (inst instance) ID() string           { return inst.id }
func (inst instance) Name() string         { return inst.workflow.name }
func (inst instance) Request() interface{} { return inst.req }
func (inst instance) StartTime() time.Time { return inst.startTime }
