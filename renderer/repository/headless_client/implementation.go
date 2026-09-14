package headless_client

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

const (
	renderTimeout               = 30 * time.Second
	interceptionQueueCapacity   = 128
	interceptionDecisionTimeout = time.Second
)

// headlessClientImpl bounds expensive Chromium processes independently from
// Kafka handler concurrency. This keeps a dynamic-fetch burst from exhausting
// the renderer host while preserving a fresh browser profile per render.
type headlessClientImpl struct {
	slots       chan struct{}
	frontierURL string
	permitHTTP  *http.Client
	allocCtx    context.Context
	cancelAlloc context.CancelFunc
}

func NewClient(concurrency int, frontierURL ...string) *headlessClientImpl {
	if concurrency < 1 {
		concurrency = 1
	}
	permitURL := ""
	if len(frontierURL) > 0 {
		permitURL = strings.TrimRight(frontierURL[0], "/")
	}
	
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.ExecPath("/usr/bin/chromium"),
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("headless", "new"),
		chromedp.Flag("disable-setuid-sandbox", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-software-rasterizer", true),
		chromedp.Flag("no-zygote", true),
	)
	allocCtx, cancelAlloc := chromedp.NewExecAllocator(context.Background(), opts...)

	// Create a root context and run an empty action to force the browser to start once.
	rootCtx, cancelRoot := chromedp.NewContext(allocCtx)
	if err := chromedp.Run(rootCtx); err != nil {
		log.Printf("Failed to initialize root chromedp context: %v", err)
	}

	return &headlessClientImpl{
		slots:       make(chan struct{}, concurrency),
		frontierURL: permitURL,
		permitHTTP:  &http.Client{Timeout: 10 * time.Second},
		allocCtx:    rootCtx,
		cancelAlloc: func() { cancelRoot(); cancelAlloc() },
	}
}

func (c *headlessClientImpl) Close() {
	if c.cancelAlloc != nil {
		c.cancelAlloc()
	}
}

func (c *headlessClientImpl) Fetch(ctx context.Context, url string) ([]byte, error) {
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	taskCtx, cancelTask := chromedp.NewContext(c.allocCtx)
	defer cancelTask()

	pausedRequests := make(chan *fetch.EventRequestPaused, interceptionQueueCapacity)
	// Listeners execute on chromedp's CDP event loop. They must only enqueue the
	// event: issuing a CDP command there can prevent the response to that command
	// from ever reaching the loop.
	chromedp.ListenTarget(taskCtx, func(event any) {
		paused, ok := event.(*fetch.EventRequestPaused)
		if !ok {
			return
		}
		select {
		case pausedRequests <- paused:
		default:
			// A saturated queue must not stall chromedp's event loop. Chromium has
			// already paused this request, so a short-lived sender is safe and keeps
			// the decision in the worker below rather than in the callback.
			go func() {
				select {
				case pausedRequests <- paused:
				case <-taskCtx.Done():
				}
			}()
		}
	})

	// Enable interception first so all subsequent network activity is covered.
	// This initial Run also creates the target, making its executor available to
	// the policy worker.
	if err := chromedp.Run(taskCtx, fetch.Enable()); err != nil {
		return nil, fmt.Errorf("enable headless request policy: %w", err)
	}

	renderCtx, cancel := context.WithTimeout(taskCtx, renderTimeout)
	defer cancel()
	// The browser target is now ready; grant the permit immediately before
	// Navigate so it governs the actual document request, not queue time.
	if err := c.acquireStartPermit(renderCtx, url); err != nil {
		return nil, fmt.Errorf("acquire origin start permit: %w", err)
	}
	policyCtx, cancelPolicy := context.WithCancel(renderCtx)
	defer cancelPolicy()
	policyDone := make(chan struct{})
	go func() {
		defer close(policyDone)
		runRequestPolicy(policyCtx, chromedp.FromContext(taskCtx).Target, pausedRequests, url)
	}()
	defer func() {
		cancelPolicy()
		<-policyDone
	}()

	var htmlContent string
	err := chromedp.Run(renderCtx,
		chromedp.Navigate(url),
		chromedp.WaitReady("body", chromedp.ByQuery),
		chromedp.OuterHTML("html", &htmlContent, chromedp.ByQuery),
	)
	if err != nil {
		return nil, fmt.Errorf("headless fetch failed for %s: %w", url, err)
	}
	return []byte(htmlContent), nil
}

func (c *headlessClientImpl) acquireStartPermit(ctx context.Context, targetURL string) error {
	if c.frontierURL == "" {
		return nil
	}
	body, err := json.Marshal(struct {
		URL string `json:"url"`
	}{URL: targetURL})
	if err != nil {
		return err
	}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.frontierURL+"/permits/start", strings.NewReader(string(body)))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := c.permitHTTP.Do(req)
		if err != nil {
			return err
		}
		var permit struct {
			RetryAtMS int64 `json:"retry_at_ms"`
		}
		_ = json.NewDecoder(response.Body).Decode(&permit)
		response.Body.Close()
		if response.StatusCode == http.StatusOK {
			return nil
		}
		if response.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("Frontier returned %s", response.Status)
		}
		wait := time.Until(time.UnixMilli(permit.RetryAtMS))
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// runRequestPolicy serializes Fetch-domain decisions outside chromedp's event
// loop. The Frontier reservation authorizes exactly one initial document request;
// redirects and all subresources need their own reservation and are blocked.
func runRequestPolicy(ctx context.Context, target cdp.Executor, pausedRequests <-chan *fetch.EventRequestPaused, allowedURL string) {
	for {
		select {
		case <-ctx.Done():
			return
		case paused := <-pausedRequests:
			if paused == nil {
				continue
			}

			decisionCtx, cancel := context.WithTimeout(cdp.WithExecutor(ctx, target), interceptionDecisionTimeout)
			if isReservedDocumentRequest(paused, allowedURL) {
				_ = fetch.ContinueRequest(paused.RequestID).Do(decisionCtx)
			} else {
				_ = fetch.FailRequest(paused.RequestID, network.ErrorReasonBlockedByClient).Do(decisionCtx)
			}
			cancel()
		}
	}
}

func isReservedDocumentRequest(paused *fetch.EventRequestPaused, allowedURL string) bool {
	return paused != nil &&
		paused.ResourceType == network.ResourceTypeDocument &&
		paused.RedirectedRequestID == "" &&
		urlsMatchForNavigation(paused.Request.URL, allowedURL)
}

// urlsMatchForNavigation accounts for harmless browser URL canonicalization
// (fragments, empty paths, host case, and default ports) without broadening the
// Frontier reservation to a different origin, path, or query string.
func urlsMatchForNavigation(requestURL, allowedURL string) bool {
	request, err := url.Parse(requestURL)
	if err != nil {
		return false
	}
	allowed, err := url.Parse(allowedURL)
	if err != nil {
		return false
	}

	canonicalizeNavigationURL(request)
	canonicalizeNavigationURL(allowed)
	return request.String() == allowed.String()
}

func canonicalizeNavigationURL(value *url.URL) {
	value.Scheme = strings.ToLower(value.Scheme)
	value.Host = strings.ToLower(value.Host)
	value.Fragment = ""
	if value.Path == "" {
		value.Path = "/"
	}
	if host, port, err := net.SplitHostPort(value.Host); err == nil &&
		((value.Scheme == "http" && port == "80") || (value.Scheme == "https" && port == "443")) {
		value.Host = host
	}
}

