// This file was auto-generated by Fern from our API Definition.

package service

import (
	bytes "bytes"
	context "context"
	fmt "fmt"
	fern "github.com/file-upload/fern"
	core "github.com/file-upload/fern/core"
	option "github.com/file-upload/fern/option"
	io "io"
	multipart "mime/multipart"
	http "net/http"
	url "net/url"
)

type Client struct {
	baseURL string
	caller  *core.Caller
	header  http.Header
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
		header: options.ToHeader(),
	}
}

func (c *Client) Post(
	ctx context.Context,
	file io.Reader,
	maybeFile io.Reader,
	request *fern.MyRequest,
	opts ...option.RequestOption,
) error {
	options := core.NewRequestOptions(opts...)

	baseURL := ""
	if c.baseURL != "" {
		baseURL = c.baseURL
	}
	if options.BaseURL != "" {
		baseURL = options.BaseURL
	}
	endpointURL := baseURL

	headers := core.MergeHeaders(c.header.Clone(), options.ToHeader())

	requestBuffer := bytes.NewBuffer(nil)
	writer := multipart.NewWriter(requestBuffer)
	fileFilename := "file_filename"
	if named, ok := file.(interface{ Name() string }); ok {
		fileFilename = named.Name()
	}
	filePart, err := writer.CreateFormFile("file", fileFilename)
	if err != nil {
		return err
	}
	if _, err := io.Copy(filePart, file); err != nil {
		return err
	}
	if maybeFile != nil {
		maybeFileFilename := "maybeFile_filename"
		if named, ok := maybeFile.(interface{ Name() string }); ok {
			maybeFileFilename = named.Name()
		}
		maybeFilePart, err := writer.CreateFormFile("maybeFile", maybeFileFilename)
		if err != nil {
			return err
		}
		if _, err := io.Copy(maybeFilePart, maybeFile); err != nil {
			return err
		}
	}
	if request.MaybeString != nil {
		if err := writer.WriteField("maybeString", fmt.Sprintf("%v", *request.MaybeString)); err != nil {
			return err
		}
	}
	if err := writer.WriteField("integer", fmt.Sprintf("%v", request.Integer)); err != nil {
		return err
	}
	if request.MaybeInteger != nil {
		if err := writer.WriteField("maybeInteger", fmt.Sprintf("%v", *request.MaybeInteger)); err != nil {
			return err
		}
	}
	if err := core.WriteMultipartJSON(writer, "listOfStrings", request.ListOfStrings); err != nil {
		return err
	}
	if err := core.WriteMultipartJSON(writer, "setOfStrings", request.SetOfStrings); err != nil {
		return err
	}
	if request.OptionalListOfStrings != nil {
		if err := core.WriteMultipartJSON(writer, "optionalListOfStrings", request.OptionalListOfStrings); err != nil {
			return err
		}
	}
	if request.OptionalSetOfStrings != nil {
		if err := core.WriteMultipartJSON(writer, "optionalSetOfStrings", request.OptionalSetOfStrings); err != nil {
			return err
		}
	}
	if err := core.WriteMultipartJSON(writer, "maybeList", request.MaybeList); err != nil {
		return err
	}
	if request.OptionalMaybeList != nil {
		if err := core.WriteMultipartJSON(writer, "optionalMaybeList", *request.OptionalMaybeList); err != nil {
			return err
		}
	}
	if err := core.WriteMultipartJSON(writer, "maybeListOrSet", request.MaybeListOrSet); err != nil {
		return err
	}
	if request.OptionalMaybeListOrSet != nil {
		if err := core.WriteMultipartJSON(writer, "optionalMaybeListOrSet", *request.OptionalMaybeListOrSet); err != nil {
			return err
		}
	}
	if err := core.WriteMultipartJSON(writer, "listOfObjects", request.ListOfObjects); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	headers.Set("Content-Type", writer.FormDataContentType())

	if err := c.caller.Call(
		ctx,
		&core.CallParams{
			URL:         endpointURL,
			Method:      http.MethodPost,
			MaxAttempts: options.MaxAttempts,
			Headers:     headers,
			Client:      options.HTTPClient,
			Request:     requestBuffer,
		},
	); err != nil {
		return err
	}
	return nil
}

func (c *Client) JustFile(
	ctx context.Context,
	file io.Reader,
	opts ...option.RequestOption,
) error {
	options := core.NewRequestOptions(opts...)

	baseURL := ""
	if c.baseURL != "" {
		baseURL = c.baseURL
	}
	if options.BaseURL != "" {
		baseURL = options.BaseURL
	}
	endpointURL := baseURL + "/" + "just-file"

	headers := core.MergeHeaders(c.header.Clone(), options.ToHeader())

	requestBuffer := bytes.NewBuffer(nil)
	writer := multipart.NewWriter(requestBuffer)
	fileFilename := "file_filename"
	if named, ok := file.(interface{ Name() string }); ok {
		fileFilename = named.Name()
	}
	filePart, err := writer.CreateFormFile("file", fileFilename)
	if err != nil {
		return err
	}
	if _, err := io.Copy(filePart, file); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	headers.Set("Content-Type", writer.FormDataContentType())

	if err := c.caller.Call(
		ctx,
		&core.CallParams{
			URL:         endpointURL,
			Method:      http.MethodPost,
			MaxAttempts: options.MaxAttempts,
			Headers:     headers,
			Client:      options.HTTPClient,
			Request:     requestBuffer,
		},
	); err != nil {
		return err
	}
	return nil
}

func (c *Client) JustFileWithQueryParams(
	ctx context.Context,
	file io.Reader,
	request *fern.JustFileWithQueryParamsRequet,
	opts ...option.RequestOption,
) error {
	options := core.NewRequestOptions(opts...)

	baseURL := ""
	if c.baseURL != "" {
		baseURL = c.baseURL
	}
	if options.BaseURL != "" {
		baseURL = options.BaseURL
	}
	endpointURL := baseURL + "/" + "just-file-with-query-params"

	queryParams := make(url.Values)
	if request.MaybeString != nil {
		queryParams.Add("maybeString", fmt.Sprintf("%v", *request.MaybeString))
	}
	queryParams.Add("integer", fmt.Sprintf("%v", request.Integer))
	if request.MaybeInteger != nil {
		queryParams.Add("maybeInteger", fmt.Sprintf("%v", *request.MaybeInteger))
	}
	for _, value := range request.ListOfStrings {
		queryParams.Add("listOfStrings", fmt.Sprintf("%v", value))
	}
	for _, value := range request.OptionalListOfStrings {
		queryParams.Add("optionalListOfStrings", fmt.Sprintf("%v", *value))
	}
	if len(queryParams) > 0 {
		endpointURL += "?" + queryParams.Encode()
	}

	headers := core.MergeHeaders(c.header.Clone(), options.ToHeader())

	requestBuffer := bytes.NewBuffer(nil)
	writer := multipart.NewWriter(requestBuffer)
	fileFilename := "file_filename"
	if named, ok := file.(interface{ Name() string }); ok {
		fileFilename = named.Name()
	}
	filePart, err := writer.CreateFormFile("file", fileFilename)
	if err != nil {
		return err
	}
	if _, err := io.Copy(filePart, file); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	headers.Set("Content-Type", writer.FormDataContentType())

	if err := c.caller.Call(
		ctx,
		&core.CallParams{
			URL:         endpointURL,
			Method:      http.MethodPost,
			MaxAttempts: options.MaxAttempts,
			Headers:     headers,
			Client:      options.HTTPClient,
			Request:     requestBuffer,
		},
	); err != nil {
		return err
	}
	return nil
}
