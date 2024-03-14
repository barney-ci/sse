/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package sse

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/cenkalti/backoff.v1"
)

var (
	headerID    = []byte("id:")
	headerData  = []byte("data:")
	headerEvent = []byte("event:")
	headerRetry = []byte("retry:")
)

func ClientMaxBufferSize(s int) func(c *Client) {
	return func(c *Client) {
		c.maxBufferSize = s
	}
}

// ConnCallback defines a function to be called on a particular connection event
type ConnCallback func(c *Client)

// FailureCallback defines a function to be called on client failure (after retries)
type FailureCallback func(c *Client, err error)

// ResponseValidator validates a response
type ResponseValidator func(c *Client, resp *http.Response) error

// Client handles an incoming server stream
type Client struct {
	Retry             time.Time
	ReconnectStrategy backoff.BackOff
	disconnectcb      ConnCallback
	connectedcb       ConnCallback
	failedcb          FailureCallback
	subCancels        map[chan *Event]context.CancelFunc
	Headers           map[string]string
	ReconnectNotify   backoff.Notify
	ResponseValidator ResponseValidator
	Connection        *http.Client
	URL               string
	LastEventID       atomic.Value // []byte
	maxBufferSize     int
	mu                sync.Mutex
	EncodingBase64    bool
	Connected         bool
}

// NewClient creates a new client
func NewClient(url string, opts ...func(c *Client)) *Client {
	c := &Client{
		URL:           url,
		Connection:    &http.Client{},
		Headers:       make(map[string]string),
		subCancels:    make(map[chan *Event]context.CancelFunc),
		maxBufferSize: 1 << 16,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Subscribe to a data stream
func (c *Client) Subscribe(stream string, handler func(msg *Event)) error {
	return c.SubscribeWithContext(context.Background(), stream, handler)
}

// SubscribeWithContext to a data stream with context
func (c *Client) SubscribeWithContext(ctx context.Context, stream string, handler func(msg *Event)) error {
	operation := func() error {
		resp, err := c.request(ctx, stream)
		if err != nil {
			return err
		}
		if validator := c.ResponseValidator; validator != nil {
			err = validator(c, resp)
			if err != nil {
				return err
			}
		} else if resp.StatusCode != 200 {
			resp.Body.Close()
			return fmt.Errorf("could not connect to stream: %s", http.StatusText(resp.StatusCode))
		}
		defer resp.Body.Close()

		// Successful connection: reset the backoff time.
		if c.ReconnectStrategy != nil {
			c.ReconnectStrategy.Reset()
		}

		reader := NewEventStreamReader(resp.Body, c.maxBufferSize)
		eventChan, errorChan := c.startReadLoop(ctx, reader)

		for {
			select {
			case err = <-errorChan:
				return err
			case msg := <-eventChan:
				handler(msg)
			}
		}
	}
	return c.retryNotify(ctx, operation)
}

// SubscribeChan sends all events to the provided channel
func (c *Client) SubscribeChan(stream string, ch chan *Event) error {
	return c.SubscribeChanWithContext(context.Background(), stream, ch)
}

// SubscribeChanWithContext sends all events to the provided channel with context
func (c *Client) SubscribeChanWithContext(ctx context.Context, stream string, ch chan *Event) error {
	var connected bool
	subCtx, cancelFunc := context.WithCancel(ctx)
	errch := make(chan error)
	c.mu.Lock()
	c.subCancels[ch] = cancelFunc
	c.mu.Unlock()

	operation := func() error {
		resp, err := c.request(subCtx, stream)
		if err != nil {
			return err
		}
		if validator := c.ResponseValidator; validator != nil {
			err = validator(c, resp)
			if err != nil {
				return err
			}
		} else if resp.StatusCode != 200 {
			resp.Body.Close()
			return fmt.Errorf("could not connect to stream: %s", http.StatusText(resp.StatusCode))
		}
		defer resp.Body.Close()

		if !connected {
			// Notify connect
			errch <- nil
			connected = true
		}

		reader := NewEventStreamReader(resp.Body, c.maxBufferSize)
		eventChan, errorChan := c.startReadLoop(subCtx, reader)

		for {
			var msg *Event
			// Wait for message to arrive or exit
			select {
			case msg = <-eventChan:
			case err = <-errorChan:
				return err
			case <-subCtx.Done():
				return nil
			}

			// Wait for message to be sent or exit
			if msg != nil {
				select {
				case <-subCtx.Done():
					return nil
				case ch <- msg:
					// message sent
				}
			}
		}
	}

	go func() {
		defer c.cleanup(ch)
		err := c.retryNotify(subCtx, operation)
		// channel closed once connected
		if err != nil && !connected {
			errch <- err
		}
	}()
	err := <-errch
	close(errch)
	return err
}

func (c *Client) retryNotify(ctx context.Context, operation func() error) error {
	var bk backoff.BackOff
	// Apply user specified reconnection strategy or default to standard NewExponentialBackOff() reconnection method
	if c.ReconnectStrategy != nil {
		bk = c.ReconnectStrategy
	} else {
		bk = backoff.NewExponentialBackOff()
	}
	bk = backoff.WithContext(bk, ctx)
	err := backoff.RetryNotify(operation, bk, c.ReconnectNotify)
	if c.failedcb != nil && err != nil {
		if ctx.Err() != nil {
			// If the context has been canceled, which can happen when a subscription is ended, return that error
			// instead of the last one encountered during backoff.
			err = ctx.Err()
		}
		c.failedcb(c, err)
	}
	return err
}

func (c *Client) startReadLoop(ctx context.Context, reader *EventStreamReader) (chan *Event, chan error) {
	outCh := make(chan *Event)
	erChan := make(chan error)
	go c.readLoop(ctx, reader, outCh, erChan)
	return outCh, erChan
}

func (c *Client) readLoop(ctx context.Context, reader *EventStreamReader, outCh chan *Event, erChan chan error) {
	for {
		msg, err := c.readLoopInner(reader)
		if err != nil {
			select {
			case <-ctx.Done():
			case erChan <- err:
			}
			break
		} else if msg != nil {
			select {
			case <-ctx.Done():
			case outCh <- msg:
			}
		}
	}
}

func (c *Client) readLoopInner(reader *EventStreamReader) (*Event, error) {
	// Read each new line and process the type of event
	event, err := reader.ReadEvent()
	if err != nil {
		// run user specified disconnect function
		if c.disconnectcb != nil {
			c.Connected = false
			c.disconnectcb(c)
		}
		return nil, err
	}

	if !c.Connected && c.connectedcb != nil {
		c.Connected = true
		c.connectedcb(c)
	}

	// If we get an error, ignore it.
	if msg, err := c.processEvent(event); err == nil {
		if len(msg.ID) > 0 {
			c.LastEventID.Store(msg.ID)
		} else {
			msg.ID, _ = c.LastEventID.Load().([]byte)
		}

		// Send downstream only if the event has something useful
		if msg.hasContent() {
			return msg, nil
		}
	}

	return nil, nil
}

// SubscribeRaw to an sse endpoint
func (c *Client) SubscribeRaw(handler func(msg *Event)) error {
	return c.Subscribe("", handler)
}

// SubscribeRawWithContext to an sse endpoint with context
func (c *Client) SubscribeRawWithContext(ctx context.Context, handler func(msg *Event)) error {
	return c.SubscribeWithContext(ctx, "", handler)
}

// SubscribeChanRaw sends all events to the provided channel
func (c *Client) SubscribeChanRaw(ch chan *Event) error {
	return c.SubscribeChan("", ch)
}

// SubscribeChanRawWithContext sends all events to the provided channel with context
func (c *Client) SubscribeChanRawWithContext(ctx context.Context, ch chan *Event) error {
	return c.SubscribeChanWithContext(ctx, "", ch)
}

// Unsubscribe unsubscribes a channel
func (c *Client) Unsubscribe(ch chan *Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cancelFunc := c.subCancels[ch]; cancelFunc != nil {
		cancelFunc()
	}
}

// OnDisconnect specifies the function to run when the connection disconnects.
// The callback will be called on _any_ disconnection event, this includes
// events caused by retries.
func (c *Client) OnDisconnect(fn ConnCallback) {
	c.disconnectcb = fn
}

// OnConnect specifies the function to run when the connection is successful.
// The callback will be called on _any_ connection event, this includes events
// caused by retries.
func (c *Client) OnConnect(fn ConnCallback) {
	c.connectedcb = fn
}

// OnFailure specifies the function to run when Retry fails, either because the
// client has run out of retry time or when the client has received permament
// error.
func (c *Client) OnFailure(fn FailureCallback) {
	c.failedcb = fn
}

func (c *Client) request(ctx context.Context, stream string) (*http.Response, error) {
	req, err := http.NewRequest("GET", c.URL, nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	// Setup request, specify stream to connect to
	if stream != "" {
		query := req.URL.Query()
		query.Add("stream", stream)
		req.URL.RawQuery = query.Encode()
	}

	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Connection", "keep-alive")

	lastID, exists := c.LastEventID.Load().([]byte)
	if exists && lastID != nil {
		req.Header.Set("Last-Event-ID", string(lastID))
	}

	// Add user specified headers
	for k, v := range c.Headers {
		req.Header.Set(k, v)
	}

	return c.Connection.Do(req)
}

func (c *Client) processEvent(msg []byte) (event *Event, err error) {
	var e Event

	if len(msg) < 1 {
		return nil, errors.New("event message was empty")
	}

	// Normalize the crlf to lf to make it easier to split the lines.
	// Split the line by "\n" or "\r", per the spec.
	for _, line := range bytes.FieldsFunc(msg, func(r rune) bool { return r == '\n' || r == '\r' }) {
		switch {
		case bytes.HasPrefix(line, headerID):
			e.ID = append([]byte(nil), trimHeader(len(headerID), line)...)
		case bytes.HasPrefix(line, headerData):
			// The spec allows for multiple data fields per event, concatenated them with "\n".
			e.Data = append(e.Data[:], append(trimHeader(len(headerData), line), byte('\n'))...)
		// The spec says that a line that simply contains the string "data" should be treated as a data field with an empty body.
		case bytes.Equal(line, bytes.TrimSuffix(headerData, []byte(":"))):
			e.Data = append(e.Data, byte('\n'))
		case bytes.HasPrefix(line, headerEvent):
			e.Event = append([]byte(nil), trimHeader(len(headerEvent), line)...)
		case bytes.HasPrefix(line, headerRetry):
			e.Retry = append([]byte(nil), trimHeader(len(headerRetry), line)...)
		default:
			// Ignore any garbage that doesn't match what we're looking for.
		}
	}

	// Trim the last "\n" per the spec.
	e.Data = bytes.TrimSuffix(e.Data, []byte("\n"))

	if c.EncodingBase64 {
		buf := make([]byte, base64.StdEncoding.DecodedLen(len(e.Data)))

		n, err := base64.StdEncoding.Decode(buf, e.Data)
		if err != nil {
			err = fmt.Errorf("failed to decode event message: %s", err)
		}
		e.Data = buf[:n]
	}
	return &e, err
}

func (c *Client) cleanup(ch chan *Event) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cancelFunc := c.subCancels[ch]
	if cancelFunc != nil {
		cancelFunc()
		delete(c.subCancels, ch)
	}
}

func trimHeader(size int, data []byte) []byte {
	if data == nil || len(data) < size {
		return data
	}

	data = data[size:]
	// Remove optional leading whitespace
	if len(data) > 0 && data[0] == 32 {
		data = data[1:]
	}
	// Remove trailing new line
	if len(data) > 0 && data[len(data)-1] == 10 {
		data = data[:len(data)-1]
	}
	return data
}
