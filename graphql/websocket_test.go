package graphql

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebSocketTerminalErrorDoesNotWaitForReceiver(t *testing.T) {
	client := NewClientUsingWebSocket("ws://unused.invalid", nil, nil).(*webSocketClient)
	want := errors.New("terminal read failure")
	done := make(chan struct{})
	go func() {
		client.handleErr(want)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		// Release the original blocked sender without hiding the failed assertion.
		require.ErrorIs(t, <-client.errChan, want)
		<-done
		t.Fatal("terminal error reporting blocked after the receiver stopped")
	}
	require.ErrorIs(t, <-client.errChan, want)
}

func TestWebSocketCloseAlwaysClosesConnection(t *testing.T) {
	injected := errors.New("injected transport write failure")
	cases := []struct {
		wantErr  error
		name     string
		failType string
	}{
		// Preserve the existing close-frame-first error contract. The fix must
		// still release the socket when a later unsubscribe cannot be written.
		{name: "close_frame_sent", wantErr: websocket.ErrCloseSent},
		{name: "close_frame_failure", failType: "close", wantErr: injected},
		{name: "unsubscribe_failure", failType: webSocketTypeComplete, wantErr: injected},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newShutdownTestServer(t)
			defer server.Close()
			dialer := &shutdownTestDialer{
				Dialer:   &websocket.Dialer{},
				failType: tc.failType, writeErr: injected,
			}
			client := NewClientUsingWebSocket("ws"+strings.TrimPrefix(server.URL, "http"), dialer, nil)
			errs, err := client.Start(context.Background())
			require.NoError(t, err)
			defer func() {
				select {
				case <-dialer.conn.closed:
				default:
					assert.NoError(t, dialer.conn.Conn.Close())
				}
			}()
			data := make(chan json.RawMessage)
			_, err = client.Subscribe(&Request{Query: "subscription { count }"}, data, nil)
			require.NoError(t, err)

			err = client.Close()
			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
			select {
			case <-dialer.conn.closed:
			default:
				t.Error("Close returned without closing the underlying connection")
			}
			select {
			case _, ok := <-errs:
				assert.False(t, ok, "Close must close the error channel")
			default:
				t.Error("Close returned without closing the error channel")
			}
		})
	}
}

func TestWebSocketCloseIsIdempotent(t *testing.T) {
	server := newShutdownTestServer(t)
	defer server.Close()
	dialer := &shutdownTestDialer{Dialer: &websocket.Dialer{}}
	client := NewClientUsingWebSocket("ws"+strings.TrimPrefix(server.URL, "http"), dialer, nil)
	_, err := client.Start(context.Background())
	require.NoError(t, err)
	require.NoError(t, client.Close())
	require.NotPanics(t, func() { assert.NoError(t, client.Close()) })
}

func TestWebSocketCloseWithoutStart(t *testing.T) {
	client := NewClientUsingWebSocket("ws://unused.invalid", nil, nil)
	require.NoError(t, client.Close())
}

func TestWebSocketPreservesAuthenticationPingAndTerminalErrors(t *testing.T) {
	for _, code := range []int{websocket.CloseNormalClosure, websocket.CloseInternalServerErr, 4403} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			protocolDone := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{Subprotocols: []string{"graphql-transport-ws"}}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Errorf("upgrade: %v", err)
					return
				}
				defer func() {
					assert.NoError(t, conn.Close())
					close(protocolDone)
				}()
				assert.Equal(t, "graphql-transport-ws", conn.Subprotocol())
				var init webSocketInitMessage
				if err := conn.ReadJSON(&init); err != nil {
					t.Errorf("init: %v", err)
					return
				}
				assert.Equal(t, map[string]interface{}{"Authorization": "Bearer fixture-token"}, init.Payload)
				if err := conn.WriteJSON(map[string]string{"type": webSocketTypeConnAck}); err != nil {
					t.Errorf("ack: %v", err)
					return
				}
				var subscribe webSocketSendMessage
				if err := conn.ReadJSON(&subscribe); err != nil {
					t.Errorf("subscribe: %v", err)
					return
				}
				assert.Equal(t, webSocketTypeSubscribe, subscribe.Type)
				if err := conn.WriteJSON(map[string]string{"type": webSocketTypePing}); err != nil {
					t.Errorf("ping: %v", err)
					return
				}
				var pong webSocketPongMessage
				if err := conn.ReadJSON(&pong); err != nil {
					t.Errorf("pong: %v", err)
					return
				}
				assert.Equal(t, webSocketTypePong, pong.Type)
				if err := conn.WriteMessage(closeMessage, formatCloseMessage(code, "fixture termination")); err != nil {
					t.Errorf("close: %v", err)
				}
			}))
			defer server.Close()
			dialer := &shutdownTestDialer{Dialer: &websocket.Dialer{}}
			client := NewClientUsingWebSocketWithConnectionParams(
				"ws"+strings.TrimPrefix(server.URL, "http"), dialer, nil,
				map[string]interface{}{"Authorization": "Bearer fixture-token"},
			)
			errs, err := client.Start(context.Background())
			require.NoError(t, err)
			_, err = client.Subscribe(&Request{Query: "subscription { count }"}, make(chan json.RawMessage), nil)
			require.NoError(t, err)
			select {
			case err := <-errs:
				assert.True(t, websocket.IsCloseError(err, code), "original close error: %v", err)
			case <-time.After(time.Second):
				t.Error("terminal close error was not reported")
			}
			_ = client.Close() // The server already closed the socket.
			<-protocolDone
		})
	}
}

// Use a real socket and inject only the selected write failure. This verifies
// cleanup even when the protocol's best-effort shutdown frames cannot be sent.
type shutdownTestDialer struct {
	writeErr error
	*websocket.Dialer
	conn     *shutdownTestConn
	failType string
}

func (d *shutdownTestDialer) DialContext(ctx context.Context, url string, header http.Header) (WSConn, error) {
	conn, response, err := d.Dialer.DialContext(ctx, url, header)
	if response != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	d.conn = &shutdownTestConn{
		Conn: conn, closed: make(chan struct{}),
		failType: d.failType, writeErr: d.writeErr,
	}
	return d.conn, nil
}

type shutdownTestConn struct {
	writeErr error
	*websocket.Conn
	closed    chan struct{}
	failType  string
	closeOnce sync.Once
}

func (c *shutdownTestConn) WriteMessage(messageType int, data []byte) error {
	if c.failType == "close" && messageType == closeMessage {
		return c.writeErr
	}
	if c.failType != "" && messageType == textMessage {
		var message struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &message); err != nil {
			return err
		}
		if message.Type == c.failType {
			return c.writeErr
		}
	}
	return c.Conn.WriteMessage(messageType, data)
}

func (c *shutdownTestConn) Close() error {
	err := c.Conn.Close()
	c.closeOnce.Do(func() { close(c.closed) })
	return err
}

func newShutdownTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{Subprotocols: []string{"graphql-transport-ws"}}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer func() { assert.NoError(t, conn.Close()) }()
		var init webSocketInitMessage
		if err := conn.ReadJSON(&init); err != nil {
			t.Errorf("init: %v", err)
			return
		}
		assert.Equal(t, webSocketTypeConnInit, init.Type)
		if err := conn.WriteJSON(map[string]string{"type": webSocketTypeConnAck}); err != nil {
			t.Errorf("ack: %v", err)
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
}
