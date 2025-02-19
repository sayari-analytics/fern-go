// This file was auto-generated by Fern from our API Definition.

package api

import (
	fmt "fmt"
	bar "github.com/example/repository/bar"
	core "github.com/example/repository/core"
	uuid "github.com/google/uuid"
)

type Foo struct {
	Name string    `json:"name"`
	Bar  *bar.Bar  `json:"bar,omitempty"`
	Uuid uuid.UUID `json:"uuid"`
}

func (f *Foo) String() string {
	if value, err := core.StringifyJSON(f); err == nil {
		return value
	}
	return fmt.Sprintf("%#v", f)
}
