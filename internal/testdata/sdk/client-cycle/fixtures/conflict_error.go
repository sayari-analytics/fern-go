// This file was auto-generated by Fern from our API Definition.

package api

import (
	json "encoding/json"
	core "github.com/fern-api/fern-go/internal/testdata/sdk/client-cycle/fixtures/core"
)

type ConflictError struct {
	*core.APIError
	Body *Error
}

func (c *ConflictError) UnmarshalJSON(data []byte) error {
	var body *Error
	if err := json.Unmarshal(data, &body); err != nil {
		return err
	}
	c.StatusCode = 409
	c.Body = body
	return nil
}

func (c *ConflictError) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Body)
}
