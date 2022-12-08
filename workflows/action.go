package workflows

// all actions complete or fail with an error
// actions can take a long time to complete
// todo: the error can be permanent or temporary
// todo: actions may be interrupted in a distributed
// system, then be retried on another instance of the engine
type Action func() (interface{}, error)
