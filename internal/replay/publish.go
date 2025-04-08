package replay

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pentops/golib/gl"
	"github.com/pentops/log.go/log"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_pb"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	ReplayEventPGChannel  = "replay_event_notification"
	ReplayUpsertPGChannel = "replay_upsert_notification"
)

type SQS interface {
	PublishBatch(context.Context, sqs.SendMessageBatchInput) (sqs.SendMessageBatchOutput, error)
}

type Message interface {
	Message() (*messaging_pb.Message, error)
	Destination() string
}

type messageBatch[T Message] struct {
	queueUrl      string
	messages      []T
	messageBodies []string
}

type messageBatches[T Message] map[string]messageBatch[T]

func (mb messageBatches[T]) addRow(row T) error {
	destination := row.Destination()
	batch, ok := mb[destination]
	if !ok {
		batch = messageBatch[T]{queueUrl: destination}
	}
	msg, err := row.Message()
	if err != nil {
		log.WithError(context.Background(), err).Error("failed to get message")

	}
	messageBody, err := protojson.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	batch.messages = append(batch.messages, row)
	batch.messageBodies = append(batch.messageBodies, string(messageBody))
	mb[destination] = batch
	return nil

}

// publishBatch publishes a batch of messages to SQS, returning the successfully
// published messages.
func publishBatch[T Message](ctx context.Context, sqsClient SQS, batch messageBatch[T]) ([]T, error) {
	log.WithField(ctx, "queueUrl", batch.queueUrl).Debug("publishing batch")

	input := sqs.SendMessageBatchInput{
		Entries:  make([]types.SendMessageBatchRequestEntry, len(batch.messageBodies)),
		QueueUrl: &batch.queueUrl,
	}

	for idx, body := range batch.messageBodies {
		input.Entries[idx] = types.SendMessageBatchRequestEntry{
			Id:          gl.Ptr(strconv.Itoa(idx)),
			MessageBody: &body,
		}
	}

	output, err := sqsClient.PublishBatch(ctx, input)
	if err != nil {

		// From the docs: The result of sending each message is reported individually in the response. Because the batch request can result in a combination of successful and unsuccessful actions, you should check for batch errors even when the call returns an HTTP status code of 200 .

		return nil, fmt.Errorf("error publishing batch: %w", err)
	}

	success := make([]T, 0, len(output.Successful))
	for _, msg := range output.Successful {
		if msg.Id == nil {
			return nil, fmt.Errorf("message ID is nil")
		}
		idx, err := strconv.Atoi(*msg.Id)
		if err != nil {
			return nil, fmt.Errorf("error converting message ID to int: %w", err)
		}
		if idx < 0 || idx >= len(batch.messages) {
			return nil, fmt.Errorf("message ID out of range: %d", idx)
		}
		success = append(success, batch.messages[idx])
	}

	return success, nil
}
