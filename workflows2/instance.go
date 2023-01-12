package workflows

// type Instance interface {
// 	YieldData(caller string) *yieldData
// }

// type instance struct {
// 	yieldByCaller map[string]*yieldData

// 	runs []*instanceRun
// }

// func newInstance() *instance {
// 	return &instance{
// 		yieldByCaller: map[string]*yieldData{},
// 		runs:          []*instanceRun{},
// 	}
// }

// func (inst instance) YieldData(caller string) *yieldData {
// 	if yd, ok := inst.yieldByCaller[caller]; ok {
// 		return yd
// 	}
// 	yd := &yieldData{
// 		caller:  caller,
// 		started: false,
// 		done:    false,
// 		result:  nil,
// 		err:     nil,
// 	}
// 	inst.yieldByCaller[caller] = yd
// 	return yd
// }

// func (inst *instance) Run(fnc func(ctx Instance) error) (*yieldData, error) {
// 	thisRun := &instanceRun{}
// 	inst.runs = append(inst.runs, thisRun)

// 	err := fnc(inst)
// 	if thisRun.yielded != nil {
// 		return thisRun.yielded, nil
// 	}
// 	if err != nil {
// 		return nil, errors.Wrapf(err, "failed and not yielded")
// 	}
// 	return nil, nil //completed successfully
// }

// type instanceRun struct {
// 	start   time.Time
// 	end     time.Time
// 	yielded *yieldData
// }

// type actionFunc func(ctx Instance, req interface{}) (res interface{}, err error)

// type CtxYieldInfoKey logger.Caller
