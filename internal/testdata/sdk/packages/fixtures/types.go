// This file was auto-generated by Fern from our API Definition.

package api

import (
	json "encoding/json"
	fmt "fmt"
	core "github.com/fern-api/fern-go/internal/testdata/sdk/packages/fixtures/core"
)

type Error struct {
	Message string `json:"message"`

	_rawJSON json.RawMessage
}

func (e *Error) UnmarshalJSON(data []byte) error {
	type unmarshaler Error
	var value unmarshaler
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*e = Error(value)
	e._rawJSON = json.RawMessage(data)
	return nil
}

func (e *Error) String() string {
	if len(e._rawJSON) > 0 {
		if value, err := core.StringifyJSON(e._rawJSON); err == nil {
			return value
		}
	}
	if value, err := core.StringifyJSON(e); err == nil {
		return value
	}
	return fmt.Sprintf("%#v", e)
}

type Foo struct {
	Id   string `json:"id"`
	Name string `json:"name"`

	_rawJSON json.RawMessage
}

func (f *Foo) UnmarshalJSON(data []byte) error {
	type unmarshaler Foo
	var value unmarshaler
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*f = Foo(value)
	f._rawJSON = json.RawMessage(data)
	return nil
}

func (f *Foo) String() string {
	if len(f._rawJSON) > 0 {
		if value, err := core.StringifyJSON(f._rawJSON); err == nil {
			return value
		}
	}
	if value, err := core.StringifyJSON(f); err == nil {
		return value
	}
	return fmt.Sprintf("%#v", f)
}
