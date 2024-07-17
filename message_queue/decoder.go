package mesage_queue

import (
	"encoding/json"
)

// Decode takes in a raw TCP message, decodes it, and returns the message as a string
// and it's type (consumer or publish)
func Decode(msg []byte) *StandardRequest {
	var standardReq StandardRequest

	err := json.Unmarshal(msg, &standardReq)

	if err != nil {
		return nil
	}

	if standardReq.Type == "PRODUCE" || standardReq.Type == "CONSUME" {
		return &standardReq
	}

	return nil
}
