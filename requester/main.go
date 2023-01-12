package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jansemmelink/j-workflow/aws"
	workflows "github.com/jansemmelink/j-workflow/workflows2"
)

func main() {
	qname := flag.String("qname", "Workflow-Test", "qname to consume in SQS")
	wname := flag.String("wname", "test", "Name of workflow")
	dataString := flag.String("data", "", "JSON data to send")
	dataFile := flag.String("file", "", "JSON file to send")
	flag.Parse()

	if *qname == "" {
		panic("missing --qname")
	}
	if *wname == "" {
		panic("missing --wname")
	}
	startEvent := workflows.Event{
		Start: &workflows.StartEvent{
			WorkflowName: *wname,
		},
		Source: "requester",
		Time:   time.Now(),
	}
	if *dataString == "" {
		if *dataFile == "" {
			panic(fmt.Sprintf("--data or --file is required"))
		}
		f, err := os.Open(*dataFile)
		if err != nil {
			panic(fmt.Sprintf("failed to open file %s: %+v", *dataFile, err))
		}
		if err := json.NewDecoder(f).Decode(&startEvent.Data); err != nil {
			f.Close()
			panic(fmt.Sprintf("failed to read JSON object from file %s: %+v", *dataFile, err))
		}
		f.Close()
	} else {
		if err := json.Unmarshal([]byte(*dataString), &startEvent.Data); err != nil {
			panic(fmt.Sprintf("--data is not valid JSON object: %+v", err))
		}
	}

	myAWS, err := aws.New()
	if err != nil {
		panic(fmt.Sprintf("failed to connect with AWS: %+v", err))
	}
	es, err := myAWS.SQS(*qname, false)
	if err != nil {
		panic(fmt.Sprintf("failed to create SQS event stream: %+v", err))
	}
	if err := es.PushStart(startEvent); err != nil {
		panic(fmt.Sprintf("failed to push start event: %+v", err))
	}
} //main()
