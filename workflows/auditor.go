package workflows

import (
	"fmt"
	"os"
	"time"
)

type Auditor interface {
	Write(
		startTime time.Time,
		endStop time.Time,
		err error,
		data interface{},
	)
}

type fileAuditor struct {
	file    *os.File
	encoder AuditEncoder
}

type AuditRecord struct {
	Start  time.Time   `json:"start"`
	End    time.Time   `json:"end"`
	Result AuditResult `json:"result"`
	Data   interface{} `json:"data"`
}

type AuditResult struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

func (fa fileAuditor) Write(
	start time.Time,
	end time.Time,
	err error,
	data interface{},
) {
	result := AuditResult{}
	if err == nil {
		result.Success = true
	} else {
		result.Success = false
		result.Error = fmt.Sprintf("%+v", err)
	}
	if encodedRecord, err := fa.encoder.Encode(AuditRecord{
		Start:  start,
		End:    end,
		Result: result,
		Data:   data,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "failed to encode audit record: %+v\n", err)
	} else {
		if _, err = fa.file.Write(encodedRecord); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write audit record: %+v\n", err)
		}
	}
}
