package skeleton

import (
	"github.com/go-resty/resty/v2"
	"net/http"
)

// RestyRoundTripper wraps a resty client for use as an HTTP client.
type RestyRoundTripper struct {
	restyClient *resty.Client
}

// RoundTrip implements the RoundTripper interface, converting the request for Resty
func (r *RestyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	restyReq := r.restyClient.R().
		SetDoNotParseResponse(true) // Important to keep the raw body open

	// Transfer headers
	for k, v := range req.Header {
		restyReq.SetHeader(k, v[0])
	}

	restyReq.Method = req.Method
	restyReq.URL = req.URL.String()

	resp, err := restyReq.Send()
	if err != nil {
		return nil, err
	}

	httpResp := &http.Response{
		StatusCode: resp.StatusCode(),
		Header:     resp.Header(),
		Body:       resp.RawBody(), // Ensure body is not prematurely closed
	}

	return httpResp, nil
}
