// Package identity provides a client for the DIMO identity-api GraphQL service.
package identity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// errDeviceNotFound is returned when the device DID is not found as either aftermarket or synthetic.
var errDeviceNotFound = errors.New("device not found")

// Client can resolve a device DID to the vehicle DID it is paired with.
type Client interface {
	// GetLinkedDIDForDevice looks up the vehicle DID that the given device DID is
	// connected to. Returns an error if the device has no paired vehicle or is unknown.
	GetLinkedDIDForDevice(ctx context.Context, deviceDID string) (string, error)
}

// HTTPClient queries the identity-api GraphQL endpoint over HTTP.
type HTTPClient struct {
	url        string
	httpClient *http.Client
}

// New creates a new HTTPClient for the given identity-api GraphQL URL.
func New(url string) *HTTPClient {
	return &HTTPClient{
		url:        url,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

type gqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

type gqlError struct {
	Message string `json:"message"`
}

// vehicleRef is the nested shape returned by both aftermarket and synthetic device queries.
type vehicleRef struct {
	Vehicle *struct {
		TokenDID string `json:"tokenDID"`
	} `json:"vehicle"`
}

// deviceResponse is the shared GraphQL response envelope for device-lookup queries.
// The Data map is keyed by the GraphQL field name (e.g. "aftermarketDevice").
type deviceResponse struct {
	Data   map[string]*vehicleRef `json:"data"`
	Errors []gqlError             `json:"errors"`
}

// GetLinkedDIDForDevice looks up the device as both aftermarket and synthetic in a single
// GraphQL request. One will typically return data and the other NOT_FOUND; the first
// with a valid vehicle DID is returned. If both fail, an error is returned.
func (c *HTTPClient) GetLinkedDIDForDevice(ctx context.Context, deviceDID string) (string, error) {
	const query = `query($did: String!) {
  aftermarketDevice(by: {tokenDID: $did}) {
    vehicle { tokenDID }
  }
  syntheticDevice(by: {tokenDID: $did}) {
    vehicle { tokenDID }
  }
}`
	var resp deviceResponse
	if err := c.doQuery(ctx, query, map[string]any{"did": deviceDID}, &resp); err != nil {
		return "", err
	}
	// Partial success is normal: one field has data, the other has a NOT_FOUND error in resp.Errors.
	for _, field := range []string{"aftermarketDevice", "syntheticDevice"} {
		device := resp.Data[field]
		if device != nil && device.Vehicle != nil && device.Vehicle.TokenDID != "" {
			return device.Vehicle.TokenDID, nil
		}
	}
	// Both null/missing vehicle.
	msg := "not found as aftermarket or synthetic device"
	if len(resp.Errors) > 0 {
		msg = resp.Errors[0].Message
	}
	return "", fmt.Errorf("%w: %s", errDeviceNotFound, msg)
}

// doQuery sends a GraphQL POST request and JSON-decodes the response into dest.
func (c *HTTPClient) doQuery(ctx context.Context, query string, vars map[string]any, dest any) error {
	body, err := json.Marshal(gqlRequest{Query: query, Variables: vars})
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer func() { _ = httpResp.Body.Close() }()

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if httpResp.StatusCode != http.StatusOK {
		return fmt.Errorf("identity-api returned HTTP %d: %s", httpResp.StatusCode, respBody)
	}

	if err := json.Unmarshal(respBody, dest); err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}
	return nil
}
