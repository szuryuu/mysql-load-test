package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Querier metrics
	GetRandomWeightedQueryLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mysql_load_test_querier_get_random_weighted_query_latency_seconds",
			Help:    "Latency of getting random weighted queries in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{},
	)

	// QuerySourceDB metrics
	QueriesFetchTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "mysql_load_test_query_source_queries_fetch_total",
			Help: "Total number of queries fetched from the database",
		},
	)

	QueryCacheHits = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "mysql_load_test_query_source_cache_hits_total",
			Help: "Total number of cache hits when fetching queries",
		},
	)

	QueryCacheMisses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "mysql_load_test_query_source_cache_misses_total",
			Help: "Total number of cache misses when fetching queries",
		},
	)

	FetchWeightsLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "mysql_load_test_query_source_fetch_weights_latency_seconds",
			Help:    "Latency of fetching query weights in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)

	// Query execution metrics
	QueryExecutionLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mysql_load_test_query_execution_latency_seconds",
			Help:    "Latency of query execution in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"}, // type can be "explain" or "execute"
	)

	QueryExecutionErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mysql_load_test_query_execution_errors_total",
			Help: "Total number of query execution errors",
		},
		[]string{"type"}, // type can be "explain" or "execute"
	)
)
