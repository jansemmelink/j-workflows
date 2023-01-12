package workflows

import (
	"time"

	"github.com/go-msvc/errors"
)

type EventStream interface {
	Chan() <-chan Event //events are processed from this chan by Workflow.Run()
	Stop()              //call to stop queing start events in the chan so that we do not start new sessions
	PushStart(Event) error
	PushContinue(Event) error
}

type Event struct {
	Start  *StartEvent            `json:"start,omitempty" doc:"Must have either start or cont"`
	Cont   *ContEvent             `json:"continue,omitempty" doc:"Must have either start or cont"`
	Source string                 `json:"source" doc:"Source sending the event (for logging only) (optional)"`
	Time   time.Time              `json:"time" doc:"Time when event was sent"`
	Data   map[string]interface{} `json:"data" doc:"Event data to store in session (optional)"`
}

func (e Event) Validate() error {
	if e.Start == nil && e.Cont == nil {
		return errors.Errorf("missing both start and cont")
	}
	if e.Start != nil && e.Cont != nil {
		return errors.Errorf("defined both start and cont")
	}
	if e.Start != nil {
		if err := e.Start.Validate(); err != nil {
			return errors.Wrapf(err, "invalid start")
		}
	}
	if e.Cont != nil {
		if err := e.Cont.Validate(); err != nil {
			return errors.Wrapf(err, "invalid cont")
		}
	}
	if e.Source == "" {
		return errors.Errorf("missing source")
	}
	if e.Time.IsZero() {
		return errors.Errorf("missing time")
	}
	return nil
} //Event.Validate()

func (e Event) IsStart() bool { return e.Start != nil }

func (e Event) IsCont() bool { return e.Cont != nil }

type StartEvent struct {
	WorkflowName string `json:"workflow" doc:"Name of workflow to start (only when session_id=\"\")"`
}

func (e StartEvent) Validate() error {
	if e.WorkflowName == "" {
		return errors.Errorf("missing workflow_name")
	}
	return nil
} //StartEvent.Validate()

type ContEvent struct {
	SessionID string `json:"id" doc:"ID of session to continue (only when workflow=\"\")"`
	ContID    string `json:"cont_id" doc:"ID of continue (generated when request is sent)"`
}

func (e ContEvent) Validate() error {
	if e.SessionID == "" {
		return errors.Errorf("missing session_id")
	}
	if e.ContID == "" {
		return errors.Errorf("missing cont_id")
	}
	return nil
} //ContEvent.Validate()
