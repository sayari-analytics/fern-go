// This file was auto-generated by Fern from our API Definition.

package api

import (
	context "context"
	fmt "fmt"
	core "github.com/fern-api/fern-go/internal/testdata/sdk/path-and-query-params/fixtures/core"
	http "net/http"
	url "net/url"
)

type UserClient interface {
	GetUser(ctx context.Context, userId string, request *GetUserRequest) (string, error)
}

func NewUserClient(opts ...core.ClientOption) UserClient {
	options := core.NewClientOptions()
	for _, opt := range opts {
		opt(options)
	}
	return &userClient{
		baseURL:    options.BaseURL,
		httpClient: options.HTTPClient,
		header:     options.ToHeader(),
	}
}

type userClient struct {
	baseURL    string
	httpClient core.HTTPClient
	header     http.Header
}

func (u *userClient) GetUser(ctx context.Context, userId string, request *GetUserRequest) (string, error) {
	baseURL := ""
	if u.baseURL != "" {
		baseURL = u.baseURL
	}
	endpointURL := fmt.Sprintf(baseURL+"/"+"users/%v", userId)

	queryParams := make(url.Values)
	if request.Shallow != nil {
		queryParams.Add("shallow", fmt.Sprintf("%v", *request.Shallow))
	}
	if len(queryParams) > 0 {
		endpointURL += "?" + queryParams.Encode()
	}

	var response string
	if err := core.DoRequest(
		ctx,
		u.httpClient,
		endpointURL,
		http.MethodGet,
		request,
		&response,
		false,
		u.header,
		nil,
	); err != nil {
		return response, err
	}
	return response, nil
}
