package config

type Config struct {
	Logging LoggingConfig `yaml:"logging"`
	Metrics MetricsConfig `yaml:"metrics"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type MetricsConfig struct {
	Enabled         bool   `yaml:"enabled"`
	HealthPath      string `yaml:"health_path"`
	MetricsPath     string `yaml:"metrics_path"`
	ListenerAddress string `yaml:"listener_address"`
}
