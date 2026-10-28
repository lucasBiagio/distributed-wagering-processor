#!/usr/bin/env bash
set -euo pipefail

queue_url() {
  awslocal sqs create-queue --queue-name "$1" --attributes 'FifoQueue=true,ContentBasedDeduplication=true' --query 'QueueUrl' --output text
}

DLQ_URL="$(queue_url 'wager-transactions-dlq.fifo')"
DLQ_ARN="$(awslocal sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)"
TRANSACTIONS_URL="$(queue_url 'wager-transactions.fifo')"
queue_url 'wager-events.fifo' >/dev/null

awslocal sqs set-queue-attributes \
  --queue-url "$TRANSACTIONS_URL" \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}"

echo "SQS FIFO queues initialized"
