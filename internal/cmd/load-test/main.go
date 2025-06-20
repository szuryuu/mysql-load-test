package main

import (
	"fmt"
	"os"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile  string
	config   Config
	validate = validator.New()
	logger   zerolog.Logger
)

func setupLogger() {
	// Configure zerolog
	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// Pretty console output
	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
		NoColor:    false,
	}

	// Create logger with context
	logger = zerolog.New(output).
		With().
		Timestamp().
		Str("app", "mysql-load-test").
		Logger()

	// Set as global logger
	log.Logger = logger
}

var rootCmd = &cobra.Command{
	Use:          "mysql-load-test",
	Short:        "MySQL load testing tool",
	Long:         `A tool for performing load tests on MySQL databases with configurable queries and reporting.`,
	SilenceUsage: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		setupLogger()
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		logger.Info().
			Str("config_file", viper.ConfigFileUsed()).
			Msg("Starting load test")

		if err := viper.Unmarshal(&config); err != nil {
			// logger.Error().
			// 	Err(err).
			// 	Msg("Failed to unmarshal config")
			return fmt.Errorf("failed to unmarshal config: %w", err)
		}

		if err := validate.Struct(config); err != nil {
			// logger.Error().
			// 	Err(err).
			// 	Interface("config", config).
			// 	Msg("Config validation failed")
			return fmt.Errorf("config validation failed: %w", err)
		}

		logger.Info().
			Str("db_dsn", maskDSN(config.DBDSN)).
			Int("count", config.Count).
			Int("concurrency", config.Concurrency).
			Str("run_mode", config.RunMode).
			Int("qps", config.QPS).
			// Str("reporting_format", config.Reporting.Format).
			// Str("reporting_file", config.Reporting.OutFile).
			Msg("Configuration loaded successfully")

		return performLoadTest()
	},
}

// maskDSN masks sensitive information in the DSN string
func maskDSN(dsn string) string {
	// Simple masking - replace password with ****
	// In a real implementation, you might want to use a proper URL parser
	// and only mask the password part
	if len(dsn) > 0 {
		return "****@****"
	}
	return ""
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")
	rootCmd.PersistentFlags().String("db-dsn", "", "Database DSN (can also be set via config file)")
	rootCmd.PersistentFlags().Int("count", 0, "Number of queries to execute (can also be set via config file)")
	rootCmd.PersistentFlags().Int("concurrency", 0, "Number of concurrent workers (can also be set via config file)")
	rootCmd.PersistentFlags().String("run-mode", "", "Run mode: sequential or random (can also be set via config file)")
	rootCmd.PersistentFlags().Int("qps", 0, "Queries per second (can also be set via config file)")
	rootCmd.PersistentFlags().String("log-level", "info", "Log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().Bool("metrics-enabled", false, "Enable Prometheus metrics server (can also be set via config file)")
	rootCmd.PersistentFlags().String("metrics-addr", ":2112", "Address to listen on for metrics server (can also be set via config file)")

	// Bind flags to viper
	viper.BindPFlag("db_dsn", rootCmd.PersistentFlags().Lookup("db-dsn"))
	viper.BindPFlag("count", rootCmd.PersistentFlags().Lookup("count"))
	viper.BindPFlag("concurrency", rootCmd.PersistentFlags().Lookup("concurrency"))
	viper.BindPFlag("run_mode", rootCmd.PersistentFlags().Lookup("run-mode"))
	viper.BindPFlag("qps", rootCmd.PersistentFlags().Lookup("qps"))
	viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))
	viper.BindPFlag("metrics.enabled", rootCmd.PersistentFlags().Lookup("metrics-enabled"))
	viper.BindPFlag("metrics.addr", rootCmd.PersistentFlags().Lookup("metrics-addr"))
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("./config")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			fmt.Printf("Error reading config file: %s\n", err)
			os.Exit(1)
		}
	}

	// Set log level from config
	if level := viper.GetString("log_level"); level != "" {
		switch level {
		case "debug":
			zerolog.SetGlobalLevel(zerolog.DebugLevel)
		case "info":
			zerolog.SetGlobalLevel(zerolog.InfoLevel)
		case "warn":
			zerolog.SetGlobalLevel(zerolog.WarnLevel)
		case "error":
			zerolog.SetGlobalLevel(zerolog.ErrorLevel)
		}
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		// logger.Error().Err(err).Msg("Application error")
		os.Exit(1)
	}
}
