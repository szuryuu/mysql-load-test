package main

import (
	"context"
	"io"
	"sort"
	"time"
)

type InternalStats struct {
	QueriesFetched  int64   `json:"queries_fetched"`
	CacheHits       int64   `json:"cache_hits"`
	CacheMisses     int64   `json:"cache_misses"`
	CacheHitRate    float64 `json:"cache_hit_rate"`
	CacheEvictions  int64   `json:"cache_evictions"`
	CacheNewItems   int64   `json:"cache_new_items"`
	FetchWeightsLat string  `json:"fetch_weights_lat"`

	Lats   []float64 `json:"lats"`
	LatP50 string    `json:"lat_p50"`
	LatP95 string    `json:"lat_p95"`
	LatP99 string    `json:"lat_p99"`
}

type ReportAggregateStat struct {
	Fastest float64 `json:"fastest"`
	Slowest float64 `json:"slowest"`
	Average float64 `json:"average"`
	QPS     float64 `json:"qps"`
	LatP50  float64 `json:"query_latency_p50"`
	LatP95  float64 `json:"query_latency_p95"`
	LatP99  float64 `json:"query_latency_p99"`
	NumRes  int64   `json:"num_res"`
}

type Report struct {
	InternalStats *InternalStats `json:"internal_stats"`

	Lats              []float64     `json:"lats"`
	Total             time.Duration `json:"total"`
	StartAt           time.Time     `json:"start_at"`
	NumRes            int64         `json:"num_res"`
	ActiveConnections int           `json:"active_connections"`
	AvgTotal          float64       `json:"avg_total"`

	Aggregates []*ReportAggregateStat `json:"aggregates"`

	w         io.Writer
	output    string
	ErrorDist map[string]int `json:"error_dist"`

	results chan *QueryResult
	done    chan bool
}

func (r *Report) aggregate() {
	if len(r.Lats) > 0 {
		totalTime := time.Since(r.StartAt)
		sort.Float64s(r.Lats)
		aggregate := &ReportAggregateStat{
			QPS:     float64(r.NumRes) / totalTime.Seconds(),
			Average: r.AvgTotal / float64(len(r.Lats)),
			NumRes:  r.NumRes,
			Fastest: r.Lats[0],
			Slowest: r.Lats[len(r.Lats)-1],
			LatP50:  r.Lats[len(r.Lats)*50/100],
			LatP95:  r.Lats[len(r.Lats)*95/100],
			LatP99:  r.Lats[len(r.Lats)*99/100],
		}
		r.insertAggregate(aggregate)

		r.StartAt = time.Now()
		r.AvgTotal = 0
		r.Lats = r.Lats[:0]
		r.NumRes = 0
	}
}

// We report for max 1M results.
const maxRes = 1000000
const maxAggregatesHistory = 100
const aggregateInterval = 5 * time.Second

func (r *Report) insertAggregate(aggregate *ReportAggregateStat) {
	if len(r.Aggregates) >= cap(r.Aggregates) {
		// shift left by one, remove the first
		r.Aggregates = append(r.Aggregates[:0], r.Aggregates[1:]...)
	} else {
		r.Aggregates = append(r.Aggregates, aggregate)
	}
}

func newReport(results chan *QueryResult) *Report {
	return &Report{
		results:       results,
		done:          make(chan bool, 1),
		ErrorDist:     make(map[string]int),
		Lats:          make([]float64, 0, maxRes),
		Aggregates:    make([]*ReportAggregateStat, 0, maxAggregatesHistory),
		InternalStats: &InternalStats{},
	}
}

func runReporter(r *Report, ctx context.Context, qds QueryDataSource, querier *Querier, metricsServer *MetricsServer) {

	ticker := time.NewTicker(aggregateInterval)
	defer ticker.Stop()

	for res := range r.results {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// collect stats from internal components

			qdsPerfStats := qds.PerfStats().(QuerySourceDBInternalPerfStats)
			querierPerfStats := querier.PerfStats()

			lats := querierPerfStats.GetRandomWeightedQueryLats()
			var p50, p95, p99 time.Duration
			if len(lats) > 0 {
				sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
				p50 = lats[len(lats)*50/100]
				p95 = lats[len(lats)*95/100]
				p99 = lats[len(lats)*99/100]
			}

			r.InternalStats.QueriesFetched = int64(qdsPerfStats.QueriesFetchTotal)
			r.InternalStats.CacheHits = int64(qdsPerfStats.CacheStats.HitsTotal)
			r.InternalStats.CacheMisses = int64(qdsPerfStats.CacheStats.MissesTotal)
			r.InternalStats.CacheHitRate = float64(qdsPerfStats.CacheStats.HitsTotal) / float64(qdsPerfStats.CacheStats.HitsTotal+qdsPerfStats.CacheStats.MissesTotal) * 100
			r.InternalStats.CacheEvictions = int64(qdsPerfStats.CacheStats.EvictionsTotal)
			r.InternalStats.CacheNewItems = int64(qdsPerfStats.CacheStats.NewItemsTotal)
			r.InternalStats.FetchWeightsLat = qdsPerfStats.FetchWeightsLat.Round(time.Millisecond).String()
			r.InternalStats.LatP50 = p50.Round(time.Millisecond).String()
			r.InternalStats.LatP95 = p95.Round(time.Millisecond).String()
			r.InternalStats.LatP99 = p99.Round(time.Millisecond).String()
			r.ActiveConnections = config.Concurrency

			r.aggregate()

			// Broadcast the report struct
			if metricsServer != nil {
				metricsServer.BroadcastStats(r)
			}

			goto collect
		default:
		}

	collect:

		r.NumRes++
		if res.Err != nil {
			r.ErrorDist[res.Err.Error()]++
		} else {
			dur := float64(res.ExecLatency.Microseconds())
			r.AvgTotal += dur
			if len(r.Lats) < maxRes {
				r.Lats = append(r.Lats, dur)
			}
		}
	}

	r.done <- true

}
