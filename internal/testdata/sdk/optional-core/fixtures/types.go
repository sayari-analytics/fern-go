// This file was auto-generated by Fern from our API Definition.

package api

import (
	json "encoding/json"
	fmt "fmt"
	core "github.com/fern-api/fern-go/internal/testdata/sdk/optional-core/fixtures/core"
)

type Optional struct {
	Value string `json:"value"`

	_rawJSON json.RawMessage
}

func (o *Optional) UnmarshalJSON(data []byte) error {
	type unmarshaler Optional
	var value unmarshaler
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*o = Optional(value)
	o._rawJSON = json.RawMessage(data)
	return nil
}

func (o *Optional) String() string {
	if len(o._rawJSON) > 0 {
		if value, err := core.StringifyJSON(o._rawJSON); err == nil {
			return value
		}
	}
	if value, err := core.StringifyJSON(o); err == nil {
		return value
	}
	return fmt.Sprintf("%#v", o)
}
