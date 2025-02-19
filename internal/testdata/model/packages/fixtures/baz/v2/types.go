// This file was auto-generated by Fern from our API Definition.

package v2

import (
	fmt "fmt"
	core "github.com/fern-api/fern-go/internal/testdata/model/packages/fixtures/core"
)

type Baz struct {
	Name string `json:"name"`
}

func (b *Baz) String() string {
	if value, err := core.StringifyJSON(b); err == nil {
		return value
	}
	return fmt.Sprintf("%#v", b)
}
