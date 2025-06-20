package main

import (
	"context"
	"fmt"
	"hash"
	"log"
	"sync"
	"sync/atomic"
	"time"

	httpclient "mysql-load-test/pkg/http_client"
	"mysql-load-test/pkg/query"

	"github.com/alitto/pond/v2"
	"github.com/bagaswh/mysql-toolkit/pkg/lexer"
	"github.com/bagaswh/mysql-toolkit/pkg/normalizer"
	"github.com/cespare/xxhash"
)

type cache[I any] struct {
	mu   sync.RWMutex
	data map[string]I
}

func NewCache[I any]() *cache[I] {
	return &cache[I]{
		data: make(map[string]I),
	}
}

func (c *cache[I]) Get(key []byte) (I, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.data[string(key)]
	return val, ok
}

func (c *cache[I]) Set(key []byte, val I) I {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[string(key)] = val
	return val
}

type ProcessorConfig struct {
	MaxConcurrency     int
	FingerprintServers []string
	ProgressInterval   time.Duration
}

type Processor struct {
	cfg            ProcessorConfig
	workerPool     pond.Pool
	httpClient     *httpclient.LoadBalancedClient
	progressTicker *time.Ticker
	progress       atomic.Int64

	rawQueriesCache       *cache[[]byte]
	rawQueriesHashCache   *cache[uint64]
	fingerprintsCache     *cache[[]byte]
	fingerprintsHashCache *cache[uint64]
	lexerPool             sync.Pool
	hasherPool            sync.Pool
	bufferPool            sync.Pool
}

func NewProcessor(cfg ProcessorConfig) (*Processor, error) {
	if cfg.MaxConcurrency <= 0 {
		return nil, fmt.Errorf("max concurrency must be greater than 0: %d", cfg.MaxConcurrency)
	}

	var httpClient *httpclient.LoadBalancedClient
	if len(cfg.FingerprintServers) > 0 {
		var err error
		httpClient, err = httpclient.NewLoadBalancedClient(cfg.FingerprintServers, nil)
		if err != nil {
			return nil, fmt.Errorf("error creating http client: %w", err)
		}
	}

	rawQueriesCache := NewCache[[]byte]()
	rawQueriesHashCache := NewCache[uint64]()
	fingerprintsCache := NewCache[[]byte]()
	fingerprintsHashCache := NewCache[uint64]()

	lexerPool := sync.Pool{
		New: func() interface{} {
			return lexer.NewLexer()
		},
	}
	hasherPool := sync.Pool{
		New: func() interface{} {
			return xxhash.New()
		},
	}
	bufferPool := sync.Pool{
		New: func() interface{} {
			// return empty buffer first
			// user will initialize the buffer themselves
			return []byte{}
		},
	}

	return &Processor{
		cfg:            cfg,
		httpClient:     httpClient,
		progressTicker: time.NewTicker(time.Second),

		rawQueriesCache:       rawQueriesCache,
		rawQueriesHashCache:   rawQueriesHashCache,
		fingerprintsCache:     fingerprintsCache,
		fingerprintsHashCache: fingerprintsHashCache,
		lexerPool:             lexerPool,
		hasherPool:            hasherPool,
		bufferPool:            bufferPool,
	}, nil
}

func (p *Processor) Close() {
	p.progressTicker.Stop()
}

func (p *Processor) startProgressReporting(ctx context.Context) {
	go func() {
		lastProgress := int64(0)
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Processing complete")
				return
			case <-p.progressTicker.C:
				progress := p.progress.Load()
				if progress > 0 {
					fmt.Printf("%d queries processed (%d/s)\n", progress, int64(progress-lastProgress))
				}
				lastProgress = progress
			}
		}
	}()
}

func normalizeAndPutToCache(q []byte, cache *cache[[]byte], config normalizer.Config, lexer *lexer.Lexer, buf []byte) ([]byte, []byte, error) {
	existing, ok := cache.Get(q)

	if !ok {
		for {
			n, _, err := normalizer.Normalize(config, lexer, q, buf)
			if err == normalizer.ErrBufferTooSmall {
				buf = make([]byte, cap(buf)*2)
				continue
			} else if err != nil {
				return q, buf, fmt.Errorf("error normalizing query: %w", err)
			}
			existing = make([]byte, n)
			copy(existing, buf[:n])
			cache.Set(q, existing)
			break
		}
	}
	return existing, buf, nil
}

func (p *Processor) processorGoroutine(ctx context.Context, inQueryChan <-chan *query.Query, outQueryChan chan<- *query.Query, errsChan chan<- error, fatalErrsChan chan<- error) {
	hasher := p.hasherPool.Get().(hash.Hash64)
	defer p.hasherPool.Put(hasher)
	lexer := p.lexerPool.Get().(*lexer.Lexer)
	defer p.lexerPool.Put(lexer)
	buf := p.bufferPool.Get().([]byte)
	defer p.bufferPool.Put(buf)

	for q := range inQueryChan {
		// 		fmt.Println(string(q.Raw))
		select {
		case <-ctx.Done():
			return
		default:
			p.incrementProgress()

			// origRaw := make([]byte, len(q.Raw))
			// copy(origRaw, q.Raw)

			q.Raw = bytesTrimSpace(q.Raw)

			if !isValidQuery(q.Raw) {
				continue
			}

			if q.CompletelyProcessed {
				continue
			}

			if buf == nil || len(q.Raw) > cap(buf) {
				// 1024 for additional space. Normalizing query might take more space.
				buf = make([]byte, len(q.Raw)+1024)
			}

			var err error
			q.Raw, buf, err = normalizeAndPutToCache(q.Raw, p.rawQueriesCache, normalizer.Config{
				KeywordCase:    normalizer.CaseLower,
				RemoveLiterals: false,
			}, lexer, buf)
			if err != nil {
				errsChan <- fmt.Errorf("error normalizing query: %w", err)
				continue
			}
			if q.Hash == 0 {
				existingHash, ok := p.rawQueriesHashCache.Get(q.Raw)
				if ok {
					q.Hash = existingHash
				} else {
					q.Hash = p.rawQueriesHashCache.Set(q.Raw, doHash(hasher, q.Raw))
				}
			}

			if q.Fingerprint == nil || len(q.Fingerprint) == 0 {
				q.Fingerprint, buf, err = normalizeAndPutToCache(q.Raw, p.fingerprintsCache, normalizer.Config{
					KeywordCase:    normalizer.CaseLower,
					RemoveLiterals: true, // fingerprinting
					// PutBacktickOnKeywords:   true,
					// PutSpaceBeforeOpenParen: true,
				}, lexer, buf)
				if err != nil {
					errsChan <- fmt.Errorf("error normalizing fingerprint for query: %w", err)
					continue
				}
			}
			if q.FingerprintHash == 0 && len(q.Fingerprint) > 0 {
				existingHash, ok := p.fingerprintsHashCache.Get(q.Fingerprint)
				if ok {
					q.FingerprintHash = existingHash
				} else {
					q.FingerprintHash = p.fingerprintsHashCache.Set(q.Fingerprint, doHash(hasher, q.Fingerprint))
				}
			}

			if !isValidFingerprint(q.Fingerprint) {
				continue
			}

			q.CompletelyProcessed = true

			outQueryChan <- q
		}
	}
}

func (p *Processor) StartProcessingQueries(ctx context.Context, inQueryChan <-chan *query.Query, outQueryChan chan<- *query.Query) error {
	newCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	p.startProgressReporting(ctx)

	errsChan := make(chan error, 100)
	fatalErrsChan := make(chan error, 1)

	var wg sync.WaitGroup
	for i := 0; i < p.cfg.MaxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.processorGoroutine(ctx, inQueryChan, outQueryChan, errsChan, fatalErrsChan)
		}()
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errsChan:
				log.Printf("Error processing query: %v\n", err)
			case err := <-fatalErrsChan:
				log.Printf("Fatal error: %v\n", err)
				return
			}
		}
	}()

	wg.Wait()
	return context.Cause(newCtx)
}

// doHash calculates a hash for the given data
func doHash(hasher hash.Hash64, data []byte) uint64 {
	hasher.Reset()
	hasher.Write(data)
	return hasher.Sum64()
}

// incrementProgress increments the progress counter
func (p *Processor) incrementProgress() {
	p.progress.Add(1)
}
