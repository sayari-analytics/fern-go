// This file was auto-generated by Fern from our API Definition.

package client

import (
	core "github.com/literal-headers/fern/core"
	noheaders "github.com/literal-headers/fern/noheaders"
	onlyliteralheaders "github.com/literal-headers/fern/onlyliteralheaders"
	option "github.com/literal-headers/fern/option"
	withnonliteralheaders "github.com/literal-headers/fern/withnonliteralheaders"
	http "net/http"
)

type Client struct {
	baseURL string
	caller  *core.Caller
	header  http.Header

	NoHeaders             *noheaders.Client
	OnlyLiteralHeaders    *onlyliteralheaders.Client
	WithNonLiteralHeaders *withnonliteralheaders.Client
}

func NewClient(opts ...option.RequestOption) *Client {
	options := core.NewRequestOptions(opts...)
	return &Client{
		baseURL: options.BaseURL,
		caller: core.NewCaller(
			&core.CallerParams{
				Client:      options.HTTPClient,
				MaxAttempts: options.MaxAttempts,
			},
		),
		header:                options.ToHeader(),
		NoHeaders:             noheaders.NewClient(opts...),
		OnlyLiteralHeaders:    onlyliteralheaders.NewClient(opts...),
		WithNonLiteralHeaders: withnonliteralheaders.NewClient(opts...),
	}
}
