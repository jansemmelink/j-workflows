package aws

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-msvc/errors"
	workflows "github.com/jansemmelink/j-workflow/workflows2"
)

func (a *myAws) SQS(qname string, receive bool) (workflows.EventStream, error) {
	a.Lock()
	defer a.Unlock()
	if a.sqs == nil {
		a.sqs = sqs.New(a.sess)
	}

	if existing, ok := a.eventStreamByQName[qname]; ok {
		return existing, nil
	}

	//todo: just show we have access
	result, err := a.sqs.ListQueues(nil)
	for i, url := range result.QueueUrls {
		log.Debugf("available queue[%d]: %s\n", i, *url)
	}

	es := &sqsEventStream{
		eventChan: make(chan workflows.Event),
		stopped:   false,
		sess:      a.sess,
		sqs:       a.sqs,
		qname:     qname,
	}
	urlResult, err := a.sqs.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(qname),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get q(%s) URL", qname)
	}
	es.qURL = urlResult.QueueUrl

	//if not receive, poller is not started
	if receive {
		go es.poller()
	}

	a.eventStreamByQName[qname] = es
	return es, nil
}

type sqsEventStream struct {
	eventChan chan workflows.Event
	stopped   bool
	sess      *session.Session
	sqs       *sqs.SQS
	qname     string
	qURL      *string
}

// poller runs in the background to poll SQS for more messages
// when it gets a message, it push into chan, which will be blocked
// if there are already messages in the chan not consumed, so this won't
// poll ahead more than 1 message at a time and it stops when Stop()
// was called
func (es sqsEventStream) poller() {
	timeout := int64(5) //seconds, max 12 hours = how long message is hidden from other consumers, i.e. processing must complete in this time
	for !es.stopped {
		log.Debugf("polling for a message ...")

		//receive on empty queue block for as long as the queue was configured in "Receive message wait time"
		//longer poll means clients wait longer and iterate fewer times on empty queue
		//if there are messages, they are returned immediately
		msgResult, err := es.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames: []*string{
				aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl:            es.qURL,
			MaxNumberOfMessages: aws.Int64(1), //increase this to do bulk processing faster
			VisibilityTimeout:   aws.Int64(timeout),
		})
		if err != nil {
			log.Errorf("no messages: %+v", err)
			continue //poll again or stop
		}

		//example:
		fmt.Printf("Received %d messages:", len(msgResult.Messages))
		for _, m := range msgResult.Messages {
			log.Debugf("received SQS message ID:%s Hdl:%s %d attrs %d msgAttrs", *m.MessageId, *m.ReceiptHandle, len(m.Attributes), len(m.MessageAttributes))
			for n, v := range m.Attributes {
				if v != nil {
					log.Debugf("  ATTR(%s): \"%s\"\n", n, *v)
				} else {
					log.Debugf("  ATTR(%s): nil\n", n)
				}
			}
			for n, a := range m.MessageAttributes {
				if a != nil {
					log.Debugf("  MSG ATTR(%s): \"%s\"\n", n, a)
				} else {
					log.Debugf("  MSG ATTR(%s): nil\n", n)
				}
			}

			//delete the message from the queue
			if _, err := es.sqs.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      es.qURL,
				ReceiptHandle: m.ReceiptHandle,
			}); err != nil {
				log.Errorf("failed to delete message from queue: %+v", err)
			}

			if m.Body == nil {
				log.Debugf("  body: nil\n")
				continue
			}

			//decode the body into an Event
			log.Debugf("  body: %s\n", *m.Body)
			var e workflows.Event
			if err := json.Unmarshal([]byte(*m.Body), &e); err != nil {
				log.Errorf("failed to decode body: %+v", err)
				continue
			}
			if err := e.Validate(); err != nil {
				log.Errorf("discard invalid event: %+v", err)
				continue
			}

			//push into channel
			//this will block until previous message started processing
			log.Debugf("Received event: %+v", e)
			es.eventChan <- e
		} //for each received message
	} //for polling
} //sqsEventStream.poller()

// events are processed from this chan by Workflow.Run()
func (es sqsEventStream) Chan() <-chan workflows.Event {
	return es.eventChan
}

// call to stop queing start events in the chan so that we do not start new sessions
func (es *sqsEventStream) Stop() {
	es.stopped = true
}

func (es sqsEventStream) PushStart(e workflows.Event) error {
	if err := e.Validate(); err != nil {
		return errors.Wrapf(err, "invalid event")
	}
	if !e.IsStart() {
		return errors.Errorf("not start event")
	}
	return es.push(e)
}

func (es sqsEventStream) PushContinue(e workflows.Event) error {
	if err := e.Validate(); err != nil {
		return errors.Wrapf(err, "invalid event")
	}
	if !e.IsCont() {
		return errors.Errorf("not continue event")
	}
	return es.push(e)
}

func (es sqsEventStream) push(e workflows.Event) error {
	jsonEvent, _ := json.Marshal(e)
	if _, err := es.sqs.SendMessage(
		&sqs.SendMessageInput{
			QueueUrl:     es.qURL,
			DelaySeconds: aws.Int64(10), //todo: remove
			MessageBody:  aws.String(string(jsonEvent)),
			// MessageAttributes: map[string]*sqs.MessageAttributeValue{
			// 	"Title": &sqs.MessageAttributeValue{
			// 		DataType:    aws.String("String"),
			// 		StringValue: aws.String("The Whistler"),
			// 	},
			// 	"Author": &sqs.MessageAttributeValue{
			// 		DataType:    aws.String("String"),
			// 		StringValue: aws.String("John Grisham"),
			// 	},
			// 	"WeeksOn": &sqs.MessageAttributeValue{
			// 		DataType:    aws.String("Number"),
			// 		StringValue: aws.String("6"),
			// 	},
			// },
		}); err != nil {
		return errors.Wrapf(err, "failed to push event")
	}
	return nil
} //sqsEventStream.push()
