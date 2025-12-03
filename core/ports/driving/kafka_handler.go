package driving

import "github.com/codecrafters-io/kafka-starter-go/core/domain"

// KafkaHandler is the driving port (input port) defined by the Core.
// This interface defines what external actors can do to drive the application.
// Rule 1: The Core MUST Define the Ports
type KafkaHandler interface {
	HandleRequest(req domain.Request) (domain.Response, error)
}

