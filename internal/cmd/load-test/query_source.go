package main

import (
	"context"
	"math/rand/v2"
)

type QueryDataSourceResult struct {
	Query string
	// Fingerprint string
}

type QueryDataSource interface {
	GetRandomWeightedQuery(context.Context) (*QueryDataSourceResult, error)
	PerfStats() any
	Init(context.Context) error
	Destroy() error
}

type QueryFingerprintData struct {
	// Fingerprint string
	Hash      uint64
	FreqTotal int64
}

type QueryFingerprintWeight struct {
	fingerprintData *QueryFingerprintData
	weight          float64
}

type QueryFingerprintWeights struct {
	weights     []*QueryFingerprintWeight
	totalWeight float64
}

func NewQueryFingerprintWeights() *QueryFingerprintWeights {
	return &QueryFingerprintWeights{
		weights: make([]*QueryFingerprintWeight, 0),
	}
}

func (qw *QueryFingerprintWeights) Add(weight float64, fingerprintData *QueryFingerprintData) {
	qw.weights = append(qw.weights, &QueryFingerprintWeight{
		fingerprintData: fingerprintData,
		weight:          weight,
	})
	qw.totalWeight += weight
}

func (qw *QueryFingerprintWeights) GetRandomWeighted() *QueryFingerprintData {
	if qw.totalWeight <= 0 || len(qw.weights) == 0 {
		return nil
	}

	r := rand.Float64() * qw.totalWeight
	cursor := 0.0

	for _, queryWeight := range qw.weights {
		cursor += queryWeight.weight
		if cursor >= r {
			return queryWeight.fingerprintData
		}
	}

	if len(qw.weights) > 0 {
		return qw.weights[len(qw.weights)-1].fingerprintData
	}

	return nil
}
