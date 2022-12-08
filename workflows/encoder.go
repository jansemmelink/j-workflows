package workflows

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"

	"github.com/go-msvc/errors"
)

type AuditEncoder interface {
	Encode(record AuditRecord) ([]byte, error)
}

type jsonAuditEncoder struct{}

func (e jsonAuditEncoder) Encode(record AuditRecord) ([]byte, error) {
	buff := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buff).Encode(record); err != nil {
		return nil, errors.Wrapf(err, "failed to encode audit record to JSON")
	}
	return buff.Bytes(), nil
}

type csvAuditEncoder struct {
	dataColumnNames []string
}

var resultType = map[bool]string{false: "FAIL", true: "DONE"}

func (e *csvAuditEncoder) Encode(record AuditRecord) ([]byte, error) {
	values := []string{
		record.Start.Format("2006-01-02T15:04:05Z00:00"),
		record.End.Format("2006-01-02T15:04:05Z00:00"),
		fmt.Sprintf("%.3f", record.End.Sub(record.Start).Seconds()),
		fmt.Sprintf(resultType[record.Result.Success]),
	}

	dataObj, ok := record.Data.(map[string]interface{})
	if !ok {
		//try to make it an object by encode to and from JSON
		jsonData, _ := json.Marshal(record.Data)
		json.Unmarshal(jsonData, &dataObj)
	}
	if len(dataObj) == 0 {
		values = append(values, "no data")
	} else {
		//todo: make sorted list of name values so that CSV columns are always in the same order
		//also allow user to specify fixed list of columns to print in custom order
		for _, v := range dataObj {
			values = append(values, fmt.Sprintf("%v", v))
		}
	}

	buff := bytes.NewBuffer(nil)
	if err := csv.NewWriter(buff).Write(values); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write CSV audit record: %+v\n", err)
	}
	return buff.Bytes(), nil
}
