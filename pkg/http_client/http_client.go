package httpclient

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
)

// LoadBalancedClient wraps http.Client with round-robin load balancing
type LoadBalancedClient struct {
	servers []string
	client  *http.Client
	counter uint64
	mu      sync.RWMutex
	reqPool sync.Pool
}

// NewLoadBalancedClient creates a new load-balanced HTTP client
func NewLoadBalancedClient(servers []string, client *http.Client) (*LoadBalancedClient, error) {
	if len(servers) == 0 {
		return nil, fmt.Errorf("at least one server must be provided")
	}

	// Validate server URLs
	for i, server := range servers {
		if _, err := url.Parse(server); err != nil {
			return nil, fmt.Errorf("invalid server URL at index %d: %v", i, err)
		}
	}

	if client == nil {
		client = &http.Client{}
	}

	lbc := &LoadBalancedClient{
		servers: make([]string, len(servers)),
		client:  client,
	}
	copy(lbc.servers, servers)

	// Initialize request pool
	lbc.reqPool.New = func() interface{} {
		return &http.Request{}
	}

	return lbc, nil
}

// nextServer returns the next server using round-robin algorithm
func (c *LoadBalancedClient) nextServer() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.servers) == 0 {
		return ""
	}

	index := atomic.AddUint64(&c.counter, 1) - 1
	return c.servers[index%uint64(len(c.servers))]
}

// copyRequest efficiently copies a request using a pooled request object
func (c *LoadBalancedClient) copyRequest(src *http.Request, newURL *url.URL) *http.Request {
	dst := c.reqPool.Get().(*http.Request)

	// Reset the pooled request
	*dst = http.Request{}

	// Copy essential fields
	dst.Method = src.Method
	dst.URL = newURL
	dst.Proto = src.Proto
	dst.ProtoMajor = src.ProtoMajor
	dst.ProtoMinor = src.ProtoMinor
	dst.Header = src.Header.Clone()
	dst.Body = src.Body
	dst.GetBody = src.GetBody
	dst.ContentLength = src.ContentLength
	dst.TransferEncoding = src.TransferEncoding
	dst.Close = src.Close
	dst.Host = src.Host
	dst.Form = src.Form
	dst.PostForm = src.PostForm
	dst.MultipartForm = src.MultipartForm
	dst.Trailer = src.Trailer
	dst.RemoteAddr = src.RemoteAddr
	dst.RequestURI = src.RequestURI
	dst.TLS = src.TLS
	dst.Cancel = src.Cancel
	dst.Response = src.Response

	// Copy context
	return dst.WithContext(src.Context())
}

// returnRequest returns a request to the pool
func (c *LoadBalancedClient) returnRequest(req *http.Request) {
	// Clear sensitive data before returning to pool
	req.Body = nil
	req.GetBody = nil
	req.Header = nil
	req.Form = nil
	req.PostForm = nil
	req.MultipartForm = nil
	req.Trailer = nil
	req.TLS = nil
	req.Cancel = nil
	req.Response = nil

	c.reqPool.Put(req)
}

// buildURL constructs the full URL with the selected server
func (c *LoadBalancedClient) buildURL(path string) (*url.URL, error) {
	server := c.nextServer()
	if server == "" {
		return nil, fmt.Errorf("no servers available")
	}

	baseURL, err := url.Parse(server)
	if err != nil {
		return nil, fmt.Errorf("invalid server URL: %v", err)
	}

	pathURL, err := url.Parse(path)
	if err != nil {
		return nil, fmt.Errorf("invalid path: %v", err)
	}

	return baseURL.ResolveReference(pathURL), nil
}

// Do executes an HTTP request using round-robin load balancing
func (c *LoadBalancedClient) Do(req *http.Request) (*http.Response, error) {
	// Build the full URL with load balancing
	fullURL, err := c.buildURL(req.URL.String())
	if err != nil {
		return nil, err
	}

	// Get a pooled request and copy the original
	newReq := c.copyRequest(req, fullURL)
	defer c.returnRequest(newReq)

	return c.client.Do(newReq)
}

// Get performs a GET request using round-robin load balancing
func (c *LoadBalancedClient) Get(path string, params url.Values) (*http.Response, error) {
	fullURL, err := c.buildURL(path)
	if err != nil {
		return nil, err
	}
	fullURL.RawQuery = params.Encode()
	return c.client.Get(fullURL.String())
}

// Post performs a POST request using round-robin load balancing
func (c *LoadBalancedClient) Post(path, contentType string, bodyReader io.Reader) (*http.Response, error) {
	fullURL, err := c.buildURL(path)
	if err != nil {
		return nil, err
	}

	return c.client.Post(fullURL.String(), contentType, bodyReader)
}

// Head performs a HEAD request using round-robin load balancing
func (c *LoadBalancedClient) Head(path string) (*http.Response, error) {
	fullURL, err := c.buildURL(path)
	if err != nil {
		return nil, err
	}
	return c.client.Head(fullURL.String())
}

// PostForm performs a POST form request using round-robin load balancing
func (c *LoadBalancedClient) PostForm(path string, data url.Values) (*http.Response, error) {
	fullURL, err := c.buildURL(path)
	if err != nil {
		return nil, err
	}
	return c.client.PostForm(fullURL.String(), data)
}

// AddServer adds a new server to the load balancer
func (c *LoadBalancedClient) AddServer(server string) error {
	if _, err := url.Parse(server); err != nil {
		return fmt.Errorf("invalid server URL: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.servers = append(c.servers, server)
	return nil
}

// RemoveServer removes a server from the load balancer
func (c *LoadBalancedClient) RemoveServer(server string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, s := range c.servers {
		if s == server {
			c.servers = append(c.servers[:i], c.servers[i+1:]...)
			return true
		}
	}
	return false
}

// GetServers returns a copy of the current server list
func (c *LoadBalancedClient) GetServers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	servers := make([]string, len(c.servers))
	copy(servers, c.servers)
	return servers
}

// SetClient updates the underlying HTTP client
func (c *LoadBalancedClient) SetClient(client *http.Client) {
	if client == nil {
		client = &http.Client{}
	}
	c.client = client
}
