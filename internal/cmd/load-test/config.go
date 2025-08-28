package main

type Config struct {
	DBDSN             string                 `mapstructure:"db_dsn" yaml:"db_dsn" validate:"required"`
	QueriesDataSource *QueryDataSourceConfig `mapstructure:"queries_data_source" yaml:"queries_data_source" validate:"required"`
	Count             int                    `mapstructure:"count" yaml:"count" validate:"omitempty"`
	Concurrency       int                    `mapstructure:"concurrency" yaml:"concurrency" validate:"omitempty,gte=0"`
	RunMode           string                 `mapstructure:"run_mode" yaml:"run_mode" validate:"required,oneof=sequential random"`
	QPS               int                    `mapstructure:"qps" yaml:"qps" validate:"omitempty,gte=0"`
	Metrics           MetricsConfig          `mapstructure:"metrics" yaml:"metrics" validate:"required"`
}

type QueryDataSourceConfig struct {
	Type                string                 `mapstructure:"type" yaml:"type" validate:"required,oneof=db,file"`
	QueryDataSourceDB   *QuerySourceDBConfig   `mapstructure:"db" yaml:"db"`
	QueryDataSourceFile *QuerySourceFileConfig `mapstructure:"file" yaml:"file"`
}

type ReportingConfig struct {
	OutFile string `mapstructure:"out_file" yaml:"out_file" validate:"required"`
	Format  string `mapstructure:"format" yaml:"format" validate:"required,oneof=json human"`
}

type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled" yaml:"enabled"`
	Addr    string `mapstructure:"addr" yaml:"addr" validate:"required_if=Enabled true"`
}
