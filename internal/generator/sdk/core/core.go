package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"mime/multipart"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	// contentType specifies the JSON Content-Type header value.
	contentType       = "application/json"
	contentTypeHeader = "Content-Type"
)

// HTTPClient is an interface for a subset of the *http.Client.
type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

// MergeHeaders merges the given headers together, where the right
// takes precedence over the left.
func MergeHeaders(left, right http.Header) http.Header {
	for key, values := range right {
		if len(values) > 1 {
			left[key] = values
			continue
		}
		if value := right.Get(key); value != "" {
			left.Set(key, value)
		}
	}
	return left
}

// WriteMultipartJSON writes the given value as a JSON part.
// This is used to serialize non-primitive multipart properties
// (i.e. lists, objects, etc).
func WriteMultipartJSON(writer *multipart.Writer, field string, value interface{}) error {
	bytes, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return writer.WriteField(field, string(bytes))
}

// APIError is a lightweight wrapper around the standard error
// interface that preserves the status code from the RPC, if any.
type APIError struct {
	err error

	StatusCode int `json:"-"`
}

// NewAPIError constructs a new API error.
func NewAPIError(statusCode int, err error) *APIError {
	return &APIError{
		err:        err,
		StatusCode: statusCode,
	}
}

// Unwrap returns the underlying error. This also makes the error compatible
// with errors.As and errors.Is.
func (a *APIError) Unwrap() error {
	if a == nil {
		return nil
	}
	return a.err
}

// Error returns the API error's message.
func (a *APIError) Error() string {
	if a == nil || (a.err == nil && a.StatusCode == 0) {
		return ""
	}
	if a.err == nil {
		return fmt.Sprintf("%d", a.StatusCode)
	}
	if a.StatusCode == 0 {
		return a.err.Error()
	}
	return fmt.Sprintf("%d: %s", a.StatusCode, a.err.Error())
}

// ErrorDecoder decodes *http.Response errors and returns a
// typed API error (e.g. *APIError).
type ErrorDecoder func(statusCode int, body io.Reader) error

type RateLimiter struct {
	mutex sync.Mutex
	// TODO: replace this with a wait until...
	wait bool
}

func NewRateLimiter() *RateLimiter {
	return &RateLimiter{}
}

func (r *RateLimiter) Block() {
	// return early if already blocked
	if r == nil || r.wait {
		return
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.wait = true
}

func (r *RateLimiter) UnBlock() {
	// return early if already unblocked
	if r == nil || !r.wait {
		return
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.wait = false
}

func (r *RateLimiter) Wait() {
	if r != nil {
		for {
			r.mutex.Lock()
			if r.wait {
				r.mutex.Unlock()
				log.Println("Waiting for rate limit to reset")
				time.Sleep(time.Second)
			} else {
				r.mutex.Unlock()
				return
			}
		}
	}
}

// Caller calls APIs and deserializes their response, if any.
type Caller struct {
	client      HTTPClient
	retrier     *Retrier
	rateLimiter *RateLimiter
}

// CallerParams represents the parameters used to constrcut a new *Caller.
type CallerParams struct {
	Client      HTTPClient
	MaxAttempts uint
}

// NewCaller returns a new *Caller backed by the given parameters.
func NewCaller(params *CallerParams, rateLimiter *RateLimiter) *Caller {
	var httpClient HTTPClient = http.DefaultClient
	if params.Client != nil {
		httpClient = params.Client
	}
	var retryOptions []RetryOption
	if params.MaxAttempts > 0 {
		retryOptions = append(retryOptions, WithMaxAttempts(params.MaxAttempts))
	}
	return &Caller{
		client:      httpClient,
		retrier:     NewRetrier(retryOptions...),
		rateLimiter: rateLimiter,
	}
}

// CallParams represents the parameters used to issue an API call.
type CallParams struct {
	URL                string
	Method             string
	MaxAttempts        uint
	Headers            http.Header
	Client             HTTPClient
	Request            interface{}
	Response           interface{}
	ResponseIsOptional bool
	ErrorDecoder       ErrorDecoder
}

// Call issues an API call according to the given call parameters.
func (c *Caller) Call(ctx context.Context, params *CallParams) error {
	req, err := newRequest(ctx, params.URL, params.Method, params.Headers, params.Request)
	if err != nil {
		return err
	}

	// If the call has been cancelled, don't issue the request.
	if err := ctx.Err(); err != nil {
		return err
	}

	client := c.client
	if params.Client != nil {
		// Use the HTTP client scoped to the request.
		client = params.Client
	}

	var retryOptions []RetryOption
	if params.MaxAttempts > 0 {
		retryOptions = append(retryOptions, WithMaxAttempts(params.MaxAttempts))
	}

	// Wait for rate limiter if needed
	c.rateLimiter.Wait()

	resp, err := c.retrier.Run(
		client.Do,
		req,
		params.ErrorDecoder,
		retryOptions...,
	)
	if err != nil {
		return err
	}

	// Close the response body after we're done.
	defer resp.Body.Close()

	// Check if the call was cancelled before we return the error
	// associated with the call and/or unmarshal the response data.
	if err := ctx.Err(); err != nil {
		return err
	}

	// If we get a 429 (Too many requests) response code or 502 and have a rate limiter setup, block other request and retry
	if c.rateLimiter != nil && (resp.StatusCode == 429 || resp.StatusCode == 502) {
		// block other requests until we can finish processing this one
		c.rateLimiter.Block()
		defer c.rateLimiter.UnBlock()

		attemptLimit := 3
		var attemptCount int
		for resp.StatusCode == 429 || resp.StatusCode == 502 {
			// close the previous response body, the defer will catch whatever we are left with after looping
			resp.Body.Close()
			var sleepTime int
			if resp.StatusCode == 502 {
				sleepTime = 30
			} else if sleepTimeStr := resp.Header.Get("Retry-After"); sleepTimeStr != "" {
				// Ideally we will have a "Retry-After" header to tell us how long to wait if it is a 429
				sleepTime, err = strconv.Atoi(sleepTimeStr)
				if err != nil {
					return fmt.Errorf("found a 'Retry-After' header and atttempted to parse it to an integer but failed. err: %v", err)
				}
			} else {
				// Without a header we will just do an exponential backoff
				if attemptCount > attemptLimit {
					// Give up after we hit the attempt limit
					break
				}
				attemptCount++
				sleepTime = int(math.Pow(2, float64(attemptCount)))
			}
			log.Printf("Waiting %vs for rate limit to recover...", sleepTime)
			time.Sleep(time.Duration(sleepTime) * time.Second)

			// re-make the request
			resp, err = c.client.Do(req)
			if err != nil {
				return err
			}
		}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decodeError(resp, params.ErrorDecoder)
	}

	// Mutate the response parameter in-place.
	if params.Response != nil {
		if writer, ok := params.Response.(io.Writer); ok {
			_, err = io.Copy(writer, resp.Body)
		} else {
			err = json.NewDecoder(resp.Body).Decode(params.Response)
		}
		if err != nil {
			if err == io.EOF {
				if params.ResponseIsOptional {
					// The response is optional, so we should ignore the
					// io.EOF error
					return nil
				}
				return fmt.Errorf("expected a %T response, but the server responded with nothing", params.Response)
			}
			return err
		}
	}

	return nil
}

// newRequest returns a new *http.Request with all of the fields
// required to issue the call.
func newRequest(
	ctx context.Context,
	url string,
	method string,
	endpointHeaders http.Header,
	request interface{},
) (*http.Request, error) {
	requestBody, err := newRequestBody(request)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, url, requestBody)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	req.Header.Set(contentTypeHeader, contentType)
	for name, values := range endpointHeaders {
		req.Header[name] = values
	}
	return req, nil
}

// newRequestBody returns a new io.Reader that represents the HTTP request body.
func newRequestBody(request interface{}) (io.Reader, error) {
	var requestBody io.Reader
	if request != nil {
		if body, ok := request.(io.Reader); ok {
			requestBody = body
		} else {
			requestBytes, err := json.Marshal(request)
			if err != nil {
				return nil, err
			}
			requestBody = bytes.NewReader(requestBytes)
		}
	}
	return requestBody, nil
}

// decodeError decodes the error from the given HTTP response. Note that
// it's the caller's responsibility to close the response body.
func decodeError(response *http.Response, errorDecoder ErrorDecoder) error {
	if errorDecoder != nil {
		// This endpoint has custom errors, so we'll
		// attempt to unmarshal the error into a structured
		// type based on the status code.
		return errorDecoder(response.StatusCode, response.Body)
	}
	// This endpoint doesn't have any custom error
	// types, so we just read the body as-is, and
	// put it into a normal error.
	bytes, err := io.ReadAll(response.Body)
	if err != nil && err != io.EOF {
		return err
	}
	if err == io.EOF {
		// The error didn't have a response body,
		// so all we can do is return an error
		// with the status code.
		return NewAPIError(response.StatusCode, nil)
	}
	return NewAPIError(response.StatusCode, errors.New(string(bytes)))
}
