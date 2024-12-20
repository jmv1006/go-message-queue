package mesage_queue

import (
	"encoding/base64"
	"encoding/json"
)

// Decode takes in a raw slice of bytes, decodes them, and returns the data as a StandardRequest
func Decode(msg []byte) *StandardRequest {
	var standardReq StandardRequest

	data, err := base64.StdEncoding.DecodeString(string(msg))

	if err != nil {
		return nil
	}

	err = json.Unmarshal(data, &standardReq)

	if err != nil {
		return nil
	}

	if standardReq.Type == "PRODUCE" || standardReq.Type == "CONSUME" {
		return &standardReq
	}

	return nil
}
