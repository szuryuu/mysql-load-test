package main

import "time"

// Config represents the main configuration structure that combines all component configurations
type AppConfig struct {
	Input InputCommonConfig `json:"input"`

	InputTsharkTxt InputTsharkTxtConfig `json:"input_tshark_txt"`
	// InputCache InputCacheConfig `json:"input_cache"`
	InputPcap InputPcapConfig `json:"input_pcap"`

	Output      OutputCommonConfig `json:"output"`
	OutputCache OutputCacheConfig  `json:"output_cache"`
	OutputDB    OutputDBConfig     `json:"output_db"`

	Processor ProcessorConfig `json:"processor"`
}

// New creates a new Config with default values
func NewAppConfig() *AppConfig {
	return &AppConfig{
		Input: InputCommonConfig{
			Encoding: "plain",
		},

		// InputCache: InputCacheConfig{},
		InputPcap: InputPcapConfig{},
		//
		Output:      OutputCommonConfig{},
		OutputCache: OutputCacheConfig{},
		//
		Processor: ProcessorConfig{
			MaxConcurrency:   10,
			ProgressInterval: 5 * time.Second,
		},
	}
}
