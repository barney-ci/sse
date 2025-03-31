/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package sse

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gopkg.in/cenkalti/backoff.v1"
)

var (
	urlPath string
	srv     *Server
	server  *httptest.Server
)

var mldata = `{
	"key": "value",
	"array": [
		1,
		2,
		3
	]
}`

func setup(empty bool) {
	// New Server
	srv = newServer()
	// Send almost-continuous string of events to the client
	go publishMsgs(srv, empty, 100000000)
}

func setupMultiline() {
	srv = newServer()
	srv.SplitData = true
	go publishMultilineMessages(srv, 100000000)
}

func setupCount(empty bool, count int) {
	srv = newServer()
	go publishMsgs(srv, empty, count)
}

func newServer() *Server {
	srv = New()

	mux := http.NewServeMux()
	mux.HandleFunc("/events", srv.ServeHTTP)
	server = httptest.NewServer(mux)
	urlPath = server.URL + "/events"

	srv.CreateStream("test")

	return srv
}

func newServer401() *Server {
	srv = New()

	mux := http.NewServeMux()
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})

	server = httptest.NewServer(mux)
	urlPath = server.URL + "/events"

	srv.CreateStream("test")

	return srv
}

func publishMsgs(s *Server, empty bool, count int) {
	for a := 0; a < count; a++ {
		if empty {
			s.Publish("test", &Event{Data: []byte("\n")})
		} else {
			s.Publish("test", &Event{Data: []byte("ping")})
		}
		time.Sleep(time.Millisecond * 50)
	}
}

func publishMultilineMessages(s *Server, count int) {
	for a := 0; a < count; a++ {
		s.Publish("test", &Event{ID: []byte("123456"), Data: []byte(mldata)})
	}
}

func cleanup() {
	server.CloseClientConnections()
	server.Close()
	srv.Close()
}

func TestClientSubscribe(t *testing.T) {
	setup(false)
	defer cleanup()

	c := NewClient(urlPath)

	events := make(chan *Event)
	var cErr error
	go func() {
		cErr = c.Subscribe("test", func(msg *Event) {
			if msg.Data != nil {
				events <- msg
				return
			}
		})
	}()

	for i := 0; i < 5; i++ {
		msg, err := wait(events, time.Second*1)
		require.Nil(t, err)
		assert.Equal(t, []byte(`ping`), msg)
	}

	assert.Nil(t, cErr)
}

func TestClientSubscribeMultiline(t *testing.T) {
	setupMultiline()
	defer cleanup()

	c := NewClient(urlPath)

	events := make(chan *Event)
	var cErr error

	go func() {
		cErr = c.Subscribe("test", func(msg *Event) {
			if msg.Data != nil {
				events <- msg
				return
			}
		})
	}()

	for i := 0; i < 5; i++ {
		msg, err := wait(events, time.Second*1)
		require.Nil(t, err)
		assert.Equal(t, []byte(mldata), msg)
	}

	assert.Nil(t, cErr)
}

func TestClientChanSubscribeEmptyMessage(t *testing.T) {
	setup(true)
	defer cleanup()

	c := NewClient(urlPath)

	events := make(chan *Event)
	err := c.SubscribeChan("test", events)
	require.Nil(t, err)

	for i := 0; i < 5; i++ {
		_, err := waitEvent(events, time.Second)
		require.Nil(t, err)
	}
}

func TestClientChanSubscribe(t *testing.T) {
	setup(false)
	defer cleanup()

	c := NewClient(urlPath)

	events := make(chan *Event)
	err := c.SubscribeChan("test", events)
	require.Nil(t, err)

	for i := 0; i < 5; i++ {
		msg, merr := wait(events, time.Second*1)
		if msg == nil {
			i--
			continue
		}
		assert.Nil(t, merr)
		assert.Equal(t, []byte(`ping`), msg)
	}
	c.Unsubscribe(events)
}

func TestClientOnDisconnect(t *testing.T) {
	setup(false)
	defer cleanup()

	c := NewClient(urlPath)

	called := make(chan struct{})
	c.OnDisconnect(func(client *Client) {
		called <- struct{}{}
	})

	go c.Subscribe("test", func(msg *Event) {})

	time.Sleep(time.Second)
	server.CloseClientConnections()

	assert.Equal(t, struct{}{}, <-called)
}

func TestClientOnConnect(t *testing.T) {
	setup(false)
	defer cleanup()

	c := NewClient(urlPath)

	called := make(chan struct{})
	c.OnConnect(func(client *Client) {
		called <- struct{}{}
	})

	go c.Subscribe("test", func(msg *Event) {})

	time.Sleep(time.Second)
	assert.Equal(t, struct{}{}, <-called)

	server.CloseClientConnections()
}

func TestClientChanReconnect(t *testing.T) {
	setup(false)
	defer cleanup()

	c := NewClient(urlPath)

	events := make(chan *Event)
	err := c.SubscribeChan("test", events)
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		if i == 5 {
			// kill connection
			server.CloseClientConnections()
		}
		msg, merr := wait(events, time.Second*1)
		if msg == nil {
			i--
			continue
		}
		assert.Nil(t, merr)
		assert.Equal(t, []byte(`ping`), msg)
	}
	c.Unsubscribe(events)
}

func TestClientOnFailure(t *testing.T) {
	setup(false)
	defer cleanup()

	c := NewClient(urlPath)
	c.ReconnectStrategy = &backoff.StopBackOff{}

	called := make(chan struct{})
	c.OnFailure(func(client *Client, err error) {
		called <- struct{}{}
	})

	go c.Subscribe("test", func(msg *Event) {})

	time.Sleep(time.Second)
	server.CloseClientConnections()

	assert.Equal(t, struct{}{}, <-called)
}

func TestClientOnFailureWithRetries(t *testing.T) {
	setup(false)
	defer cleanup()

	c := NewClient(urlPath)
	c.Connection.Timeout = time.Millisecond
	b := backoff.NewExponentialBackOff()
	// 1+2+4+8+16 = 31 => 5 retries
	b.RandomizationFactor = 0
	b.Multiplier = 2
	b.MaxElapsedTime = 31 * time.Millisecond
	b.InitialInterval = time.Millisecond
	c.ReconnectStrategy = b

	// make place for 6 calls. We should only receive 1 call here but we are
	// making place for 6 to check if we are not calling OnFailure multiple
	// times here. (5 retries +  1 failure call = 6 eventual failure calls)
	called := make(chan struct{}, 6)
	c.OnFailure(func(client *Client, err error) {
		called <- struct{}{}
	})
	c.ReconnectNotify = func(err error, d time.Duration) {
		t.Log(err)
	}
	c.OnDisconnect(func(client *Client) {
		t.Log("disconnected")
	})

	go c.Subscribe("test", func(msg *Event) {})

	server.CloseClientConnections()
	time.Sleep(time.Second)

	// Only 1 failure call is allowed
	assert.Len(t, called, 1)
}

func TestClientChanReconnectOnEOF(t *testing.T) {
	setup(false)
	defer cleanup()
	streamID := "test"

	c := NewClient(urlPath)

	var (
		reconnectErr      error
		reconnectDuration time.Duration
	)
	c.ReconnectStrategy = backoff.NewConstantBackOff(time.Millisecond * 10)
	c.ReconnectNotify = func(err error, duration time.Duration) {
		reconnectErr = err
		reconnectDuration = duration
	}

	events := make(chan *Event)
	err := c.SubscribeChan(streamID, events)
	require.Nil(t, err)

	msg, err := wait(events, time.Millisecond*100)
	assert.NoError(t, err)
	assert.Equal(t, []byte(`ping`), msg)

	srv.RemoveStream(streamID)
	srv.CreateStream(streamID)
	msg, err = wait(events, time.Millisecond*100)
	assert.NoError(t, err)
	assert.Equal(t, io.EOF, reconnectErr)
	assert.Equal(t, time.Millisecond*10, reconnectDuration)
	assert.Equal(t, []byte(`ping`), msg)

	c.Unsubscribe(events)
}

func TestClientUnsubscribe(t *testing.T) {
	setup(false)
	defer cleanup()

	c := NewClient(urlPath)

	events := make(chan *Event)
	err := c.SubscribeChan("test", events)
	require.Nil(t, err)

	time.Sleep(time.Millisecond * 500)

	go c.Unsubscribe(events)
	go c.Unsubscribe(events)
}

func TestClientUnsubscribeNonBlock(t *testing.T) {
	count := 2
	setupCount(false, count)
	defer cleanup()

	c := NewClient(urlPath)

	events := make(chan *Event)
	err := c.SubscribeChan("test", events)
	require.Nil(t, err)

	// Read count messages from the channel
	for i := 0; i < count; i++ {
		msg, merr := wait(events, time.Second*1)
		assert.Nil(t, merr)
		assert.Equal(t, []byte(`ping`), msg)
	}
	// No more data is available to be read in the channel
	// Make sure Unsubscribe returns quickly
	doneCh := make(chan *Event)
	go func() {
		var e Event
		c.Unsubscribe(events)
		doneCh <- &e
	}()
	_, merr := wait(doneCh, time.Millisecond*100)
	assert.Nil(t, merr)
}

func TestClientUnsubscribe401(t *testing.T) {
	srv = newServer401()
	defer cleanup()

	c := NewClient(urlPath)

	// limit retries to 3
	c.ReconnectStrategy = backoff.WithMaxTries(
		backoff.NewExponentialBackOff(),
		3,
	)

	err := c.SubscribeRaw(func(ev *Event) {
		// this shouldn't run
		assert.False(t, true)
	})

	require.NotNil(t, err)
}

func TestClientLargeData(t *testing.T) {
	srv = newServer()
	defer cleanup()

	c := NewClient(urlPath, ClientMaxBufferSize(1<<19))

	// limit retries to 3
	c.ReconnectStrategy = backoff.WithMaxTries(
		backoff.NewExponentialBackOff(),
		3,
	)

	// allocate 128KB of data to send
	data := make([]byte, 1<<17)
	rand.Read(data)
	data = []byte(hex.EncodeToString(data))

	ec := make(chan *Event, 1)

	srv.Publish("test", &Event{Data: data})

	go func() {
		c.Subscribe("test", func(ev *Event) {
			ec <- ev
		})
	}()

	d, err := wait(ec, time.Second)
	require.Nil(t, err)
	require.Equal(t, data, d)
}

func TestClientComment(t *testing.T) {
	srv = newServer()
	defer cleanup()

	c := NewClient(urlPath)

	events := make(chan *Event)
	err := c.SubscribeChan("test", events)
	require.Nil(t, err)

	srv.Publish("test", &Event{Comment: []byte("comment")})
	srv.Publish("test", &Event{Data: []byte("test")})

	ev, err := waitEvent(events, time.Second*1)
	assert.Nil(t, err)
	assert.Equal(t, []byte("test"), ev.Data)

	c.Unsubscribe(events)
}

func TestTrimHeader(t *testing.T) {
	tests := []struct {
		input []byte
		want  []byte
	}{
		{
			input: []byte("data: real data"),
			want:  []byte("real data"),
		},
		{
			input: []byte("data:real data"),
			want:  []byte("real data"),
		},
		{
			input: []byte("data:"),
			want:  []byte(""),
		},
	}

	for _, tc := range tests {
		got := trimHeader(len(headerData), tc.input)
		require.Equal(t, tc.want, got)
	}
}

func TestSubscribeWithContextDone(t *testing.T) {
	setup(false)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	n1 := runtime.NumGoroutine()

	c := NewClient(urlPath)

	for i := 0; i < 10; i++ {
		go c.SubscribeWithContext(ctx, "test", func(msg *Event) {})
	}

	time.Sleep(1 * time.Second)
	cancel()

	time.Sleep(1 * time.Second)
	n2 := runtime.NumGoroutine()

	assert.Equal(t, n1, n2)
}

func TestSubscribeWithContextAbortRetrier(t *testing.T) {
	// Run a server that only responds with HTTP errors which will put the client into the
	// backoff.RetryNotify loop.
	const status = http.StatusBadGateway
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Log(r.Method, r.URL.String(), http.StatusText(status))
		w.WriteHeader(status)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	c := NewClient(srv.URL)
	c.ReconnectNotify = backoff.Notify(func(err error, d time.Duration) {
		t.Logf("ReconnectNotify err: %v, duration: %s", err, d.String())
		// The client has processed the HTTP server error from above, so cancel the context
		// for the SubscribeWithContext call.
		cancel()
	})

	err := c.SubscribeWithContext(ctx, "test", func(msg *Event) {
		t.Fatal("Received event when none was expected:", msg)
	})
	require.Error(t, err)
	assert.Regexp(t, `could not connect to stream: `+http.StatusText(status), err.Error())
}

func subscribeChanWithContextUnsubscribeRace(ch chan error) {
	srv := New()
	mux := http.NewServeMux()
	mux.HandleFunc("/events", srv.ServeHTTP)
	server := httptest.NewServer(mux)
	urlPath := server.URL + "/events"
	srv.CreateStream("test")
	defer srv.Close()

	// Timeout for finding deadlock
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	c := NewClient(urlPath)

	eventChan := make(chan *Event)
	err := c.SubscribeChanWithContext(ctx, "test", eventChan)
	if err != nil {
		ch <- err
		return
	}

	// Simulate some messages, server automatically will close the stream
	// after it has gone through the buffer
	srv.Publish("test", &Event{Event: []byte("data"), Data: []byte("I've seen things you people wouldn't believe.")})
	srv.Publish("test", &Event{Event: []byte("data"), Data: []byte("Attack ships on fire off the shoulder of Orion.")})
	srv.Publish("test", &Event{Event: []byte("data"), Data: []byte("I watched C-beams glitter in the dark near the Tannhäuser Gate.")})
	srv.Publish("test", &Event{Event: []byte("data"), Data: []byte("All those moments will be lost in time, like tears in rain.")})
	srv.Publish("test", &Event{Event: []byte("finished"), Data: []byte(" ")})

loop:
	for {
		select {
		case ev, ok := <-eventChan:
			if !ok {
				srv.Close()
				break loop
			}
			if bytes.Equal(ev.Event, []byte("finished")) {
				srv.Close()
				break loop
			}
		case <-ctx.Done():
			ch <- fmt.Errorf("test deadline exceeded")
			return
		}
	}
	// Empirically set sleep to cause race condition
	time.Sleep(time.Microsecond * 10)
	c.Unsubscribe(eventChan)
	ch <- nil
}

func TestSubscribeChanWithContextUnsubscribeRace(t *testing.T) {
	for i := 0; i < 200; i++ {
		ch := make(chan error, 1)
		go subscribeChanWithContextUnsubscribeRace(ch)
		select {
		case err := <-ch:
			if !assert.NoError(t, err) {
				return
			}
		case <-time.After(time.Second * 2):
			assert.Fail(t, "Should return in time, possible deadlock")
			return
		}
	}
}

func TestSubscribeDisconnection(t *testing.T) {
}
