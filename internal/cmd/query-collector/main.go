// Package cmd implements the command-line interface for the mysql-load-test tool.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"mysql-load-test/pkg/query"

	_ "net/http/pprof"
)

type CollectCmd struct {
	cfg *AppConfig
}

func NewImportCmd(cfg *AppConfig) *CollectCmd {
	return &CollectCmd{
		cfg: cfg,
	}
}

func createInput(cfg *AppConfig, inputCommon *InputCommon) (Input, error) {
	switch cfg.Input.Type {
	case "tshark-txt":
		return NewInputTsharkTxt(cfg.InputTsharkTxt, inputCommon)
	case "pcap":
		return NewInputPcap(cfg.InputPcap, inputCommon)
	default:
		return nil, fmt.Errorf("unsupported input type: %s", cfg.Input.Type)
	}
}

func createOutput(cfg *AppConfig, outputCommon *OutputCommon) (Output, error) {
	switch cfg.Output.Type {
	case "cache":
		return NewCacheOutput(cfg.OutputCache, outputCommon)
	case "db":
		return NewDBOutput(cfg.OutputDB)
	case "stats":
		return NewOutputStats(), nil
	default:
		return nil, fmt.Errorf("unsupported output type: %s", cfg.Output.Type)
	}
}

func (c *CollectCmd) Execute() error {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	extractedQueriesChan := make(chan *query.Query, 1_000_000)
	processedQueriesChan := make(chan *query.Query, 1_000_000)

	// input
	inCommon := NewInputCommon(InputCommonConfig{
		Type:     c.cfg.Input.Type,
		Encoding: c.cfg.Input.Encoding,
	})
	in, err := createInput(c.cfg, inCommon)
	if err != nil {
		return fmt.Errorf("error creating input: %w", err)
	}
	defer in.Destroy()
	go func() {
		if err := in.StartExtractor(ctx, extractedQueriesChan); err != nil {
			cancel(fmt.Errorf("error extracting queries: %w", err))
			return
		}
		fmt.Println("Extraction completed")
		close(extractedQueriesChan)
	}()

	// processor
	proc, err := NewProcessor(ProcessorConfig{
		MaxConcurrency:   c.cfg.Processor.MaxConcurrency,
		ProgressInterval: c.cfg.Processor.ProgressInterval,
	})
	if err != nil {
		return fmt.Errorf("error creating processor: %w", err)
	}
	defer proc.Close()
	go func() {
		if err := proc.StartProcessingQueries(ctx, extractedQueriesChan, processedQueriesChan); err != nil {
			cancel(fmt.Errorf("error processing queries: %w", err))
			return
		}
		fmt.Println("Processor completed")
		close(processedQueriesChan)
	}()

	// output
	if c.cfg.Output.Type != "" {
		outCommon := NewOutputCommon(OutputCommonConfig{
			Type:     c.cfg.Output.Type,
			Encoding: c.cfg.Output.Encoding,
		})
		out, err := createOutput(c.cfg, outCommon)
		if err != nil {
			return fmt.Errorf("error creating output: %w", err)
		}
		defer out.Destroy()
		go func() {
			if err := out.StartOutput(ctx, processedQueriesChan); err != nil {
				cancel(fmt.Errorf("error starting output: %w", err))
				return
			}
			fmt.Println("Output completed")
			cancel(nil)
		}()
	} else {
		fmt.Fprintf(os.Stderr, "WARNING: since no output is configured, the processed queries will be discarded\n")
		for range processedQueriesChan {
		}
		fmt.Println("Output completed")
		cancel(nil)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)

	select {
	case <-ctx.Done():
		fmt.Printf("Received interrupt, exiting...\n")
		if err := context.Cause(ctx); err != nil && !errors.Is(err, context.Canceled) {
			fmt.Printf("Cause: %s\n", err.Error())
			return err
		}
		return nil
	case <-signalChan:
		fmt.Println("Received SIGTERM/SIGINT, exiting...")
		cancel(nil)
		return nil
	}
}

// NewCommand creates a new cobra command for importing queries
func NewCommand() *cobra.Command {
	cfg := NewAppConfig()

	cmd := &cobra.Command{
		Use:          "collect",
		Short:        "Collect queries from input file",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {

			// importnName, _ := cmd.Flags().GetString("import-name")

			cfg.Input.Encoding, _ = cmd.Flags().GetString("input.encoding")
			cfg.Input.Type, _ = cmd.Flags().GetString("input.type")

			cfg.InputTsharkTxt.File, _ = cmd.Flags().GetString("input.tshark-txt.file")

			// cfg.InputCache.File, _ = cmd.Flags().GetString("input.cache.file")
			// cfg.InputCache.ImportName = importnName

			cfg.InputPcap.File, _ = cmd.Flags().GetString("input.pcap.file")

			cfg.Processor.MaxConcurrency, _ = cmd.Flags().GetInt("processor.max-concurrency")
			cfg.Processor.ProgressInterval, _ = cmd.Flags().GetDuration("processor.progress-interval")
			cfg.Processor.FingerprintServers = []string{"http://localhost:6617"}

			cfg.Output.Encoding, _ = cmd.Flags().GetString("output.encoding")
			cfg.Output.Type, _ = cmd.Flags().GetString("output.type")

			cfg.OutputCache.File, _ = cmd.Flags().GetString("output.cache.file")

			cfg.OutputDB.Host, _ = cmd.Flags().GetString("output.db.host")
			cfg.OutputDB.Port, _ = cmd.Flags().GetInt("output.db.port")
			cfg.OutputDB.User, _ = cmd.Flags().GetString("output.db.user")
			cfg.OutputDB.Password, _ = cmd.Flags().GetString("output.db.pass")
			cfg.OutputDB.DBName, _ = cmd.Flags().GetString("output.db.name")
			cfg.OutputDB.Truncate, _ = cmd.Flags().GetBool("output.db.truncate")
			cfg.OutputDB.BatchSize, _ = cmd.Flags().GetInt("output.db.batch-size")

			return NewImportCmd(cfg).Execute()
		},
	}

	// input
	cmd.Flags().String("input.type", "", "Type of the input file (cache, pcap)")
	cmd.Flags().String("input.encoding", "", "Encoding of the input file (plain, gzip, zstd)")

	// input.tshark-txt
	cmd.Flags().String("input.tshark-txt.file", "", "Path to the tshark-txt file containing queries")

	// input.cache
	cmd.Flags().String("input.cache.file", "", "Path to the cache file containing queries")

	// input.pcap
	cmd.Flags().String("input.pcap.file", "", "Path to the pcap file containing queries")

	// processor
	cmd.Flags().Int("processor.max-concurrency", runtime.NumCPU(), "Maximum number of concurrent workers")
	cmd.Flags().Duration("processor.progress-interval", 5*time.Second, "Interval for reporting progress")

	// output
	cmd.Flags().String("output.encoding", "", "Encoding of the output file (plain, gzip, zstd)")
	cmd.Flags().String("output.type", "", "Type of the output file (cache)")

	cmd.Flags().String("output.cache.file", "", "Path to the cache file containing queries")

	// output db
	cmd.Flags().String("output.db.host", "", "Host of the database")
	cmd.Flags().Int("output.db.port", 3306, "Port of the database")
	cmd.Flags().String("output.db.user", "", "Username of the database")
	cmd.Flags().String("output.db.pass", "", "Password of the database")
	cmd.Flags().String("output.db.name", "", "Name of the database")
	cmd.Flags().Bool("output.db.truncate", false, "Truncate tables before inserting queries")
	cmd.Flags().Int("output.db.batch-size", 1000, "Maximum number of queries to insert in a single batch")

	// Mark required flags
	cmd.MarkFlagRequired("input.type")
	cmd.MarkFlagRequired("input.encoding")
	// cmd.MarkFlagRequired("import-name")

	return cmd
}

func main() {
	cpuProfilerFileOutput := os.Getenv("CPU_PROFILER_FILE_OUTPUT")
	if cpuProfilerFileOutput != "" {
		f, err := os.Create(cpuProfilerFileOutput)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	memProfilerFileOutput := os.Getenv("MEM_PROFILER_FILE_OUTPUT")
	if memProfilerFileOutput != "" {
		f, err := os.Create(memProfilerFileOutput)
		if err != nil {
			log.Fatal(err)
		}
		writeTicker := time.NewTicker(time.Second)
		defer writeTicker.Stop()
		go func() {
			select {
			case <-ctx.Done():
				pprof.WriteHeapProfile(f)
				f.Sync()
				return
			case <-writeTicker.C:
				pprof.WriteHeapProfile(f)
				f.Sync()
			}
		}()
	}

	if err := NewCommand().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
