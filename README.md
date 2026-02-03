# MySQL Load Test

A high-performance, Go-based toolkit designed to stress test MySQL databases by replaying real-world traffic captured from PCAP files or query logs. This project provides a complete workflow from query collection to execution, coupled with real-time visualization.

---

## Key Features

-   **Real-Traffic Replay**  
    Capture production traffic directly from network packets (PCAP) or text logs and replay them to simulate realistic load patterns rather than synthetic benchmarks.

-   **High-Performance Load Generator**  
    Built with Go’s lightweight concurrency primitives (Goroutines) to generate massive throughput with low resource overhead, capable of saturating database connections efficiently.

-   **Real-time Web Dashboard**  
    Includes a built-in web server and UI that provides instant visibility into test performance, visualizing metrics like Queries Per Second (QPS), Latency (P50/P90/P99), and Error Rates.

-   **Query Fingerprinting & Analysis**  
    Tools to normalize and fingerprint queries, allowing you to analyze query distribution, frequency weights, and resource consumption before running the test.

## Suite Components

| Component | Path | Description |
| :--- | :--- | :--- |
| **Load Tester** | `cmd/load-test` | The core engine that executes queries against the target DB, collects metrics, and serves the Web UI. |
| **Query Collector** | `cmd/query-collector` | Parses raw input (PCAP files, Text logs) to extract, normalize, and save valid SQL queries for the load test. |
| **Weights Stats** | `cmd/query-weights-stats` | Analyzes the collected query dataset to calculate execution weights and distribution statistics. |

## Quick Start

### Prerequisites
* Go 1.21+
* `libpcap-dev` (Required for PCAP collection)
* Docker (Optional, for integration testing)

1.  Collect Queries (Optional)  
    Extract queries from a tcpdump file to create a realistic test dataset.

    ```bash
    go run internal/cmd/query-collector/main.go \
    -input production_traffic.pcap \
    -output queries.txt \
    -type pcap
    ```
2.  Configure Load Test
    Modify the configuration file `config/load-test.yml` to define your target database and query source.
    
    ```yaml
    # Connection string for the MySQL database you want to stress test
    db_dsn: "admin:password@tcp(localhost:3306)/target_db?parseTime=true"

    # Control the load intensity
    concurrency: 50           # Number of parallel connections/workers
    qps: 0                    # Rate limit (0 = unlimited)
    count: -1                 # Total queries to run (-1 = infinite)
    run_mode: "random"        # Execution order: "random" or "sequential"

    # Source of the SQL queries to replay
    queries_data_source:
    type: "file"            # Use "file" to read directly from the collector output
    file:
        path: "queries.txt"

    # Metrics exposition for the Web Dashboard
    metrics:
        enabled: true
        addr: ":2112"
    ```
3.  Run the Load Test
    Execute the load tester, pointing it to your configuration file.

    ```bash
    go run internal/cmd/load-test/main.go --config config/load-test.yml
    ```
4.  Monitor Results
    The tool will output logs to `stdout`. To view real-time performance metrics, open the web dashboard:

    -   Dashboard URL: http://localhost:2112 (or the port configured in metrics.addr)
    -   Metrics Available: QPS, Latency (P99/P50), and Error Rates. 

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

