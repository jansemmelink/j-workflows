package workflows

import (
	"time"

	"github.com/go-msvc/errors"
	"github.com/google/uuid"
)

type SessionManager interface {
	New(name string) (Context, error)
	Sync(SavedContext) error
	Get(id string) (Context, error)
	Del(id string) error
}

// external session storage using an object store
func NewSessions(buckets Buckets, bucketName string) (SessionManager, error) {
	bucket, err := buckets.Bucket(bucketName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get bucket for workflow sessions")
	}
	sm := &extSessions{
		bucket:     bucket,
		bucketName: bucketName,
	}
	return sm, nil
}

type extSessions struct {
	bucket     Bucket
	bucketName string
}

func (sm extSessions) New(name string) (Context, error) {
	ctx := &workflowContext{
		//todo: move this to runContext?
		//Context:           context.Background(),

		manager:           sm,
		name:              name,
		id:                uuid.New().String(),
		yieldDataByCaller: map[string]*yieldData{},
		runs:              []runContext{},
	}

	//create the first run context
	ctx.run = &runContext{
		//Context: context.Background()???  add when needed here or else in workflowContext... this this should rather be passed to action funcs
		workflowContext: ctx,
		attemptNr:       1,
		startTime:       time.Now(),
	}

	//not yet save as this session only exists internally until it starts to wait
	//todo: it is possible for this session to send an event which might be processed elsewhere and then
	//that send a response and another workflow engine pick it up before this one synced its session
	//we should save the session here, then the other side will not discard the event, but it should
	// also not process it until this one completed its current event, so save in a way that says we're working on it
	//once we complete processing, we must update the session saying it is ready to process a new event,
	//and store the called and attempt nr it expects...
	//if receive early event while session is busy, put it back in the queue with a short delay

	return ctx, nil
}

func (sm extSessions) Sync(savedCtx SavedContext) error {
	if err := sm.bucket.Set(sm.bucketName, savedCtx.ID, savedCtx); err != nil {
		return errors.Wrapf(err, "failed to save context")
	}
	return nil
} //extSessions.Sync()

func (sm extSessions) Get(id string) (Context, error) {
	savedValue, err := sm.bucket.Get(sm.bucketName, id, SavedContext{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to retrieve context")
	}
	log.Debugf("retrieved ctx: %+v", savedValue)
	ctx := savedValue.(SavedContext).Context()
	ctx.manager = sm
	return ctx, nil
} //extSessions.Get()

func (sm extSessions) Del(id string) error {
	if err := sm.bucket.Del(sm.bucketName, id); err != nil {
		return errors.Wrapf(err, "failed to delete context")
	}
	return nil
} //extSession.Del()
